// Interpreter Neo4j Schema - Phase 1
// Based on Claude Opus design specification

// =============================================================================
// CONSTRAINTS AND INDEXES
// =============================================================================

// Temporal Entities
CREATE CONSTRAINT moment_id IF NOT EXISTS FOR (m:Moment) REQUIRE m.id IS UNIQUE;
CREATE INDEX moment_timestamp IF NOT EXISTS FOR (m:Moment) ON (m.timestamp);

// Actor Entities
CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT system_id IF NOT EXISTS FOR (s:System) REQUIRE s.id IS UNIQUE;
CREATE INDEX user_last_active IF NOT EXISTS FOR (u:User) ON (u.last_active);

// Semantic Entities
CREATE CONSTRAINT intent_id IF NOT EXISTS FOR (i:Intent) REQUIRE i.id IS UNIQUE;
CREATE CONSTRAINT decision_id IF NOT EXISTS FOR (d:Decision) REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT fact_id IF NOT EXISTS FOR (f:Fact) REQUIRE f.id IS UNIQUE;
CREATE INDEX intent_type IF NOT EXISTS FOR (i:Intent) ON (i.type);
CREATE INDEX decision_active IF NOT EXISTS FOR (d:Decision) ON (d.active);
CREATE INDEX fact_domain IF NOT EXISTS FOR (f:Fact) ON (f.domain);

// Conversation Entities
CREATE CONSTRAINT channel_id IF NOT EXISTS FOR (c:Channel) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT thread_id IF NOT EXISTS FOR (t:Thread) REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT session_id IF NOT EXISTS FOR (s:Session) REQUIRE s.id IS UNIQUE;

// Event Entities (references to TimescaleDB)
CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE;
CREATE INDEX event_time IF NOT EXISTS FOR (e:Event) ON (e.event_time);

// =============================================================================
// SAMPLE DATA MODEL (for Phase 1 testing)
// =============================================================================

// Create a sample moment
MERGE (m:Moment {
    id: "moment_2026-02-13T23:00:00Z",
    timestamp: datetime("2026-02-13T23:00:00Z"),
    window_start: datetime("2026-02-13T23:00:00Z"),
    window_end: datetime("2026-02-13T23:00:30Z")
});

// =============================================================================
// COMMON QUERY PATTERNS
// =============================================================================

// Query 1: Find all decisions made by a user
// MATCH (u:User {id: $user_id})-[:MADE]->(d:Decision)
// WHERE d.active = true
// RETURN d.statement, d.confidence, d.decided_at
// ORDER BY d.decided_at DESC;

// Query 2: Trace decision back to source events
// MATCH (d:Decision {id: $decision_id})-[r:EXTRACTED_FROM]->(e:Event)
// RETURN e.id, e.event_time, r.event_ids
// ORDER BY e.event_time;

// Query 3: Find related intents in a thread
// MATCH (t:Thread {id: $thread_id})<-[:PART_OF]-(i:Intent)
// RETURN i.type, i.text, i.confidence
// ORDER BY i.extracted_at;

// Query 4: Get user's recent activity
// MATCH (u:User {id: $user_id})-[:PARTICIPATES_IN]->(t:Thread)
// WHERE t.last_activity > datetime() - duration('P1D')
// RETURN t.id, t.topic, t.phase, t.last_activity
// ORDER BY t.last_activity DESC
// LIMIT 10;

// Query 5: Find decision negations
// MATCH (d1:Decision)-[:NEGATES]->(d2:Decision)
// WHERE d2.id = $decision_id
// RETURN d1.statement, d1.decided_at, d2.statement
// ORDER BY d1.decided_at DESC;

// =============================================================================
// HELPER QUERIES
// =============================================================================

// Clear all data (use with caution!)
// MATCH (n) DETACH DELETE n;

// Count all nodes by label
// CALL db.labels() YIELD label
// CALL apoc.cypher.run('MATCH (:`'+label+'`) RETURN count(*) as count', {})
// YIELD value
// RETURN label, value.count ORDER BY value.count DESC;

// Show schema
// CALL db.schema.visualization();
