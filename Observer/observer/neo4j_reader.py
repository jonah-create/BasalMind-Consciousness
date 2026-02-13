"""
Observer Neo4j Reader - Star Schema Queries
Optimized for dimensional queries on the Observer's event schema

Star Schema Structure:
- FACT: EventType (center of the star)
- DIMENSIONS: SourceSystem, EventCategory, Priority, RetentionPolicy,
              EventFilter, CognitiveLayer
"""

from neo4j import GraphDatabase
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class EventTypeQuery:
    """Query parameters for event type lookups."""
    name: Optional[str] = None  # Support lookup by event type name
    source_system: Optional[str] = None
    category: Optional[str] = None
    priority: Optional[str] = None
    is_active: Optional[bool] = True


class ObserverNeo4jReader:
    """
    Reader for Observer's dimensional star schema in Neo4j.

    All queries are optimized for the star schema where EventType is the fact table.
    """

    def __init__(self, neo4j_uri='bolt://localhost:7687',
                 neo4j_user='neo4j', neo4j_password='shadowcaster_secure_2024'):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        logger.info(f"ObserverNeo4jReader connected to {neo4j_uri}")


    def get_event_type_metadata(self, event_type_name: str) -> Dict[str, Any]:
        """
        Get basic metadata for a single event type by name.
        Optimized for Observer enrichment (simpler than full dimensional query).
        
        Returns:
            {
                "name": "test.enrichment_test",
                "priority": "high",
                "category": "test",
                "retention_days": 90,
                "consumed_by": ["Observer", "Interpreter"],
                "is_active": true
            }
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $name})
                OPTIONAL MATCH (et)-[:HAS_PRIORITY]->(p:Priority)
                OPTIONAL MATCH (et)-[:CATEGORIZED_AS]->(ec:EventCategory)
                OPTIONAL MATCH (et)-[:GOVERNED_BY]->(rp:RetentionPolicy)
                OPTIONAL MATCH (et)-[:CONSUMED_BY]->(cl:CognitiveLayer)
                
                WITH et, p, ec, rp, collect(DISTINCT cl.name) as layers
                
                RETURN {
                    name: et.name,
                    priority: coalesce(p.level, et.priority, 'medium'),
                    category: coalesce(ec.name, et.category, 'general'),
                    retention_days: coalesce(rp.retention_days, 30),
                    consumed_by: layers,
                    is_active: coalesce(et.is_active, true),
                    description: et.description
                } as metadata
            """, {'name': event_type_name})
            
            record = result.single()
            return record['metadata'] if record else {}

    def get_event_types_by_dimensions(self, query: EventTypeQuery) -> List[Dict[str, Any]]:
        """
        Query event types using dimensional filters.

        Example:
            query = EventTypeQuery(source_system="slack", priority="critical")
            event_types = reader.get_event_types_by_dimensions(query)

        Returns list of event types with all dimensional context.
        """
        with self.driver.session() as session:
            # Build WHERE clause dynamically
            where_clauses = []
            params = {}

            if query.name:
                where_clauses.append("et.name = $name")
                params["name"] = query.name

            if query.source_system:
                where_clauses.append("ss.name = $source_system")
                params['source_system'] = query.source_system

            if query.category:
                where_clauses.append("ec.name = $category")
                params['category'] = query.category

            if query.priority:
                where_clauses.append("p.level = $priority")
                params['priority'] = query.priority

            if query.is_active is not None:
                where_clauses.append("et.is_active = $is_active")
                params['is_active'] = query.is_active

            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            cypher = f"""
                MATCH (et:EventType)
                OPTIONAL MATCH (et)-[:BELONGS_TO]->(ss:SourceSystem)
                OPTIONAL MATCH (et)-[:CATEGORIZED_AS]->(ec:EventCategory)
                OPTIONAL MATCH (et)-[:HAS_PRIORITY]->(p:Priority)
                OPTIONAL MATCH (et)-[:GOVERNED_BY]->(rp:RetentionPolicy)
                OPTIONAL MATCH (et)-[:CONSUMED_BY]->(cl:CognitiveLayer)
                OPTIONAL MATCH (et)-[:FILTERED_BY]->(ef:EventFilter)

                {where_clause}

                RETURN et {{
                    .*,
                    source_system: ss.name,
                    category: ec.name,
                    priority: p.level,
                    retention_policy: rp {{
                        retention_days: rp.retention_days,
                        action: rp.action,
                        archive_location: rp.archive_location
                    }},
                    consumed_by_layers: collect(DISTINCT cl.name),
                    filters: collect(DISTINCT ef {{
                        name: ef.name,
                        filter_type: ef.filter_type,
                        pattern: ef.pattern
                    }})
                }} as event_type
                ORDER BY et.name
            """

            result = session.run(cypher, params)
            return [r['event_type'] for r in result]

    def get_source_system_summary(self, source_system_name: str) -> Dict[str, Any]:
        """
        Get complete summary of a source system with all its event types.

        Returns:
            {
                "name": "slack",
                "vendor": "Salesforce",
                "is_active": true,
                "event_types": [...],
                "total_events": 2,
                "by_priority": {"critical": 1, "high": 1}
            }
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (ss:SourceSystem {name: $name})
                OPTIONAL MATCH (ss)<-[:BELONGS_TO]-(et:EventType)
                OPTIONAL MATCH (et)-[:HAS_PRIORITY]->(p:Priority)

                WITH ss, et, p
                ORDER BY et.name

                WITH ss,
                     collect(DISTINCT et {
                         .*,
                         priority: p.level
                     }) as event_types,
                     collect(DISTINCT p.level) as priorities

                RETURN {
                    name: ss.name,
                    vendor: ss.vendor,
                    is_active: ss.is_active,
                    typical_event_volume: ss.typical_event_volume,
                    ingestion_method: ss.ingestion_method,
                    ingestion_frequency: ss.ingestion_frequency,
                    event_types: event_types,
                    total_events: size(event_types),
                    priorities: priorities,
                    by_priority: apoc.map.fromLists(
                        priorities,
                        [p in priorities | size([et in event_types WHERE et.priority = p])]
                    )
                } as summary
            """, {'name': source_system_name})

            record = result.single()
            return record['summary'] if record else {}

    def get_events_for_cognitive_layer(self, layer_name: str) -> List[Dict[str, Any]]:
        """
        Get all event types consumed by a specific cognitive layer.

        Args:
            layer_name: One of: Observer, Interpreter, Philosopher, Cartographer,
                       Sentinel, Assembler, Basal

        Returns:
            List of event types with full dimensional context.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (cl:CognitiveLayer {name: $layer_name})
                MATCH (cl)<-[:CONSUMED_BY]-(et:EventType)

                OPTIONAL MATCH (et)-[:BELONGS_TO]->(ss:SourceSystem)
                OPTIONAL MATCH (et)-[:HAS_PRIORITY]->(p:Priority)
                OPTIONAL MATCH (et)-[:GOVERNED_BY]->(rp:RetentionPolicy)
                OPTIONAL MATCH (et)-[:CATEGORIZED_AS]->(ec:EventCategory)

                RETURN et {
                    .*,
                    source_system: ss.name,
                    priority: p.level,
                    category: ec.name,
                    retention_days: rp.retention_days
                } as event_type
                ORDER BY p.level DESC, et.name
            """, {'layer_name': layer_name})

            return [r['event_type'] for r in result]

    def get_retention_policy_for_event(self, event_type_name: str) -> Dict[str, Any]:
        """
        Get retention policy details for a specific event type.

        Returns:
            {
                "retention_days": 90,
                "action": "archive",
                "archive_location": "s3://basalmind-archive/events",
                "applies_to": ["slack.message", "slack.app_mention"]
            }
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $event_type_name})
                      -[:GOVERNED_BY]->(rp:RetentionPolicy)

                OPTIONAL MATCH (rp)<-[:GOVERNED_BY]-(other_et:EventType)

                RETURN rp {
                    .*,
                    applies_to: collect(DISTINCT other_et.name)
                } as policy
            """, {'event_type_name': event_type_name})

            record = result.single()
            return record['policy'] if record else {}

    def should_process_event(self, event_type_name: str,
                            originated_from: str = None) -> tuple[bool, Optional[str]]:
        """
        Check if an event should be processed based on filters in Neo4j.
        Implements circular reference protection using EventFilter nodes.

        Returns:
            (should_process: bool, reason: Optional[str])
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $event_type_name})

                // Check if event type is active
                WITH et,
                     CASE WHEN et.is_active = false
                     THEN 'Event type is inactive'
                     ELSE null END as inactive_reason

                // Check for exclusion filters
                OPTIONAL MATCH (et)-[:FILTERED_BY]->(ef:EventFilter)
                WHERE ef.filter_type = 'exclude'

                // Check for loop prevention filters
                OPTIONAL MATCH (cl:CognitiveLayer {name: $originated_from})
                      -[:APPLIES_FILTER]->(loop_filter:EventFilter)
                WHERE loop_filter.filter_type = 'loop_prevention'
                  AND any(pattern IN loop_filter.excluded_patterns
                          WHERE $event_type_name STARTS WITH pattern)

                WITH et, inactive_reason,
                     collect(DISTINCT ef.reason) as exclusion_reasons,
                     collect(DISTINCT loop_filter.name) as loop_reasons

                RETURN {
                    should_process: inactive_reason IS NULL
                                   AND size(exclusion_reasons) = 0
                                   AND size(loop_reasons) = 0,
                    reason: CASE
                        WHEN inactive_reason IS NOT NULL THEN inactive_reason
                        WHEN size(loop_reasons) > 0 THEN 'Loop prevention: ' + loop_reasons[0]
                        WHEN size(exclusion_reasons) > 0 THEN exclusion_reasons[0]
                        ELSE null
                    END
                } as decision
            """, {
                'event_type_name': event_type_name,
                'originated_from': originated_from
            })

            record = result.single()
            if record:
                decision = record['decision']
                return decision['should_process'], decision['reason']

            # Default to processing if no record found
            return True, None

    def get_critical_events(self) -> List[Dict[str, Any]]:
        """
        Get all critical priority events across all source systems.
        Useful for monitoring and alerting.
        """
        query = EventTypeQuery(priority="critical", is_active=True)
        return self.get_event_types_by_dimensions(query)

    def get_active_source_systems(self) -> List[Dict[str, Any]]:
        """
        Get all active source systems with their event counts.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (ss:SourceSystem {is_active: true})
                OPTIONAL MATCH (ss)<-[:BELONGS_TO]-(et:EventType {is_active: true})

                WITH ss, count(et) as event_count
                ORDER BY event_count DESC

                RETURN ss {
                    .*,
                    active_event_types: event_count
                } as source_system
            """)

            return [r['source_system'] for r in result]

    def get_dimensional_analysis(self) -> Dict[str, Any]:
        """
        Get complete dimensional analysis of the event schema.
        Useful for dashboards and monitoring.

        Returns:
            {
                "total_event_types": 8,
                "by_source": {"slack": 2, "cloudflare": 2, ...},
                "by_priority": {"critical": 5, "high": 1, "medium": 2},
                "by_category": {...},
                "by_layer": {"Observer": 8, "Interpreter": 5, ...}
            }
        """
        with self.driver.session() as session:
            result = session.run("""
                // Aggregate across all dimensions
                MATCH (et:EventType {is_active: true})
                OPTIONAL MATCH (et)-[:BELONGS_TO]->(ss:SourceSystem)
                OPTIONAL MATCH (et)-[:HAS_PRIORITY]->(p:Priority)
                OPTIONAL MATCH (et)-[:CATEGORIZED_AS]->(ec:EventCategory)
                OPTIONAL MATCH (et)-[:CONSUMED_BY]->(cl:CognitiveLayer)

                WITH count(DISTINCT et) as total,
                     collect(DISTINCT ss.name) as sources,
                     collect(DISTINCT p.level) as priorities,
                     collect(DISTINCT ec.name) as categories,
                     collect(DISTINCT cl.name) as layers,
                     collect(DISTINCT {
                         event: et.name,
                         source: ss.name,
                         priority: p.level,
                         category: ec.name
                     }) as events

                RETURN {
                    total_event_types: total,
                    by_source: apoc.map.fromLists(
                        sources,
                        [s in sources | size([e in events WHERE e.source = s])]
                    ),
                    by_priority: apoc.map.fromLists(
                        priorities,
                        [p in priorities | size([e in events WHERE e.priority = p])]
                    ),
                    by_category: apoc.map.fromLists(
                        categories,
                        [c in categories | size([e in events WHERE e.category = c])]
                    ),
                    by_layer: apoc.map.fromLists(
                        layers,
                        [l in layers | size([e in events | l])]
                    ),
                    sources: sources,
                    priorities: priorities,
                    categories: categories,
                    layers: layers
                } as analysis
            """)

            record = result.single()
            return record['analysis'] if record else {}

    def close(self):
        """Clean up Neo4j connection."""
        self.driver.close()
        logger.info("ObserverNeo4jReader connection closed")


# Example usage
if __name__ == "__main__":
    print("\n📊 Observer Neo4j Reader - Star Schema Queries")
    print("=" * 80)

    reader = ObserverNeo4jReader()

    # 1. Query by dimensions
    print("\n1️⃣ Query Critical Slack Events:")
    query = EventTypeQuery(source_system="slack", priority="critical")
    events = reader.get_event_types_by_dimensions(query)
    for event in events:
        print(f"   - {event['name']}: {event.get('description', 'N/A')}")

    # 2. Source system summary
    print("\n2️⃣ Cloudflare Source System Summary:")
    summary = reader.get_source_system_summary("cloudflare")
    if summary:
        print(f"   Vendor: {summary.get('vendor')}")
        print(f"   Total Events: {summary.get('total_events')}")
        print(f"   By Priority: {summary.get('by_priority')}")

    # 3. Events for cognitive layer
    print("\n3️⃣ Events for Observer Layer:")
    observer_events = reader.get_events_for_cognitive_layer("Observer")
    print(f"   Observer consumes {len(observer_events)} event types")

    # 4. Check if should process event
    print("\n4️⃣ Loop Prevention Check:")
    should_process, reason = reader.should_process_event(
        "internal.observer_heartbeat",
        originated_from="Observer"
    )
    print(f"   Process 'internal.observer_heartbeat'? {should_process}")
    print(f"   Reason: {reason}")

    # 5. Dimensional analysis
    print("\n5️⃣ Complete Dimensional Analysis:")
    analysis = reader.get_dimensional_analysis()
    if analysis:
        print(f"   Total Event Types: {analysis.get('total_event_types')}")
        print(f"   By Source: {analysis.get('by_source')}")
        print(f"   By Priority: {analysis.get('by_priority')}")

    reader.close()
    print("\n✅ Demo complete")
