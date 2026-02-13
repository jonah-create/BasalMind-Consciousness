"""
Observer Neo4j Writer - Multi-dimensional Writes
Optimized for writing to the Observer's star schema

Star Schema Structure:
- FACT: EventType (center of the star)
- DIMENSIONS: SourceSystem, EventCategory, Priority, RetentionPolicy,
              EventFilter, CognitiveLayer

All writes maintain referential integrity and create proper dimensional relationships.
"""

from neo4j import GraphDatabase
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class EventTypeDefinition:
    """Complete definition for creating an event type with all dimensions."""
    name: str
    description: str
    source_system: str
    category: str
    priority: str
    retention_policy: str
    schema_version: str = "1.0"
    is_active: bool = True
    consumed_by_layers: Optional[List[str]] = None
    filters: Optional[List[str]] = None
    sample_payload: Optional[Dict[str, Any]] = None


@dataclass
class SourceSystemDefinition:
    """Definition for creating a source system."""
    name: str
    vendor: str
    typical_event_volume: str
    ingestion_method: str
    ingestion_frequency: str
    is_active: bool = True
    credentials: Optional[Dict[str, str]] = None


class ObserverNeo4jWriter:
    """
    Writer for Observer's dimensional star schema in Neo4j.

    All writes maintain referential integrity across dimensions.
    """

    def __init__(self, neo4j_uri='bolt://localhost:7687',
                 neo4j_user='neo4j', neo4j_password='shadowcaster_secure_2024'):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        logger.info(f"ObserverNeo4jWriter connected to {neo4j_uri}")

    def create_event_type(self, definition: EventTypeDefinition) -> Dict[str, Any]:
        """
        Create an event type with all dimensional relationships.

        This is a multi-dimensional write that:
        1. Creates/updates the EventType fact node
        2. Connects to all required dimension nodes
        3. Validates referential integrity
        4. Returns complete event type with all relationships

        Example:
            definition = EventTypeDefinition(
                name="slack.message",
                description="User messages in Slack channels",
                source_system="slack",
                category="user_interaction",
                priority="high",
                retention_policy="standard_90d",
                consumed_by_layers=["Observer", "Interpreter"]
            )
            result = writer.create_event_type(definition)
        """
        with self.driver.session() as session:
            result = session.run("""
                // 1. Create or update EventType (FACT)
                MERGE (et:EventType {name: $name})
                SET et.description = $description,
                    et.schema_version = $schema_version,
                    et.is_active = $is_active,
                    et.sample_payload = $sample_payload,
                    et.updated_at = datetime()

                // 2. Connect to SourceSystem dimension
                WITH et
                MATCH (ss:SourceSystem {name: $source_system})
                MERGE (et)-[:BELONGS_TO]->(ss)

                // 3. Connect to EventCategory dimension
                WITH et
                MATCH (ec:EventCategory {name: $category})
                MERGE (et)-[:CATEGORIZED_AS]->(ec)

                // 4. Connect to Priority dimension
                WITH et
                MATCH (p:Priority {level: $priority})
                MERGE (et)-[:HAS_PRIORITY]->(p)

                // 5. Connect to RetentionPolicy dimension
                WITH et
                MATCH (rp:RetentionPolicy {name: $retention_policy})
                MERGE (et)-[:GOVERNED_BY]->(rp)

                // 6. Connect to CognitiveLayer dimensions (optional)
                WITH et
                UNWIND CASE WHEN $consumed_by_layers IS NOT NULL
                            THEN $consumed_by_layers ELSE [] END as layer_name
                MATCH (cl:CognitiveLayer {name: layer_name})
                MERGE (et)-[:CONSUMED_BY]->(cl)

                // 7. Connect to EventFilter dimensions (optional)
                WITH et
                UNWIND CASE WHEN $filters IS NOT NULL
                            THEN $filters ELSE [] END as filter_name
                MATCH (ef:EventFilter {name: filter_name})
                MERGE (et)-[:FILTERED_BY]->(ef)

                // 8. Return complete event type with all dimensions
                WITH et
                MATCH (et)-[:BELONGS_TO]->(ss:SourceSystem)
                MATCH (et)-[:CATEGORIZED_AS]->(ec:EventCategory)
                MATCH (et)-[:HAS_PRIORITY]->(p:Priority)
                MATCH (et)-[:GOVERNED_BY]->(rp:RetentionPolicy)
                OPTIONAL MATCH (et)-[:CONSUMED_BY]->(cl:CognitiveLayer)
                OPTIONAL MATCH (et)-[:FILTERED_BY]->(ef:EventFilter)

                RETURN et {
                    .*,
                    source_system: ss.name,
                    category: ec.name,
                    priority: p.level,
                    retention_policy: rp.name,
                    consumed_by: collect(DISTINCT cl.name),
                    filters: collect(DISTINCT ef.name)
                } as event_type
            """, {
                'name': definition.name,
                'description': definition.description,
                'schema_version': definition.schema_version,
                'is_active': definition.is_active,
                'sample_payload': str(definition.sample_payload) if definition.sample_payload else None,
                'source_system': definition.source_system,
                'category': definition.category,
                'priority': definition.priority,
                'retention_policy': definition.retention_policy,
                'consumed_by_layers': definition.consumed_by_layers,
                'filters': definition.filters
            })

            record = result.single()
            if record:
                logger.info(f"Created/updated event type: {definition.name}")
                return record['event_type']
            else:
                logger.error(f"Failed to create event type: {definition.name}")
                return {}

    def create_source_system(self, definition: SourceSystemDefinition) -> Dict[str, Any]:
        """
        Create a source system dimension node.

        Example:
            definition = SourceSystemDefinition(
                name="github",
                vendor="Microsoft",
                typical_event_volume="medium",
                ingestion_method="webhook",
                ingestion_frequency="real-time"
            )
            result = writer.create_source_system(definition)
        """
        with self.driver.session() as session:
            result = session.run("""
                MERGE (ss:SourceSystem {name: $name})
                SET ss.vendor = $vendor,
                    ss.typical_event_volume = $typical_event_volume,
                    ss.ingestion_method = $ingestion_method,
                    ss.ingestion_frequency = $ingestion_frequency,
                    ss.is_active = $is_active,
                    ss.updated_at = datetime()

                // Store credentials if provided (should be encrypted in production)
                WITH ss
                FOREACH (ignored IN CASE WHEN $credentials IS NOT NULL THEN [1] ELSE [] END |
                    SET ss.credentials = $credentials
                )

                RETURN ss {.*} as source_system
            """, {
                'name': definition.name,
                'vendor': definition.vendor,
                'typical_event_volume': definition.typical_event_volume,
                'ingestion_method': definition.ingestion_method,
                'ingestion_frequency': definition.ingestion_frequency,
                'is_active': definition.is_active,
                'credentials': str(definition.credentials) if definition.credentials else None
            })

            record = result.single()
            if record:
                logger.info(f"Created/updated source system: {definition.name}")
                return record['source_system']
            else:
                logger.error(f"Failed to create source system: {definition.name}")
                return {}

    def create_event_filter(self, name: str, filter_type: str, pattern: str = None,
                           excluded_patterns: List[str] = None,
                           max_depth: int = None, reason: str = None) -> Dict[str, Any]:
        """
        Create an EventFilter dimension for loop prevention and filtering.

        Args:
            name: Filter name (e.g., "exclude_observer_self_events")
            filter_type: One of: "exclude", "loop_prevention", "rate_limit"
            pattern: Regex pattern for matching (optional)
            excluded_patterns: List of patterns to exclude (for loop prevention)
            max_depth: Maximum recursion depth (for loop prevention)
            reason: Human-readable reason for the filter
        """
        with self.driver.session() as session:
            result = session.run("""
                MERGE (ef:EventFilter {name: $name})
                SET ef.filter_type = $filter_type,
                    ef.pattern = $pattern,
                    ef.excluded_patterns = $excluded_patterns,
                    ef.max_depth = $max_depth,
                    ef.reason = $reason,
                    ef.created_at = datetime()

                RETURN ef {.*} as filter
            """, {
                'name': name,
                'filter_type': filter_type,
                'pattern': pattern,
                'excluded_patterns': excluded_patterns,
                'max_depth': max_depth,
                'reason': reason
            })

            record = result.single()
            if record:
                logger.info(f"Created/updated event filter: {name}")
                return record['filter']
            else:
                return {}

    def link_filter_to_layer(self, filter_name: str, layer_name: str) -> bool:
        """
        Link an EventFilter to a CognitiveLayer.
        Used for loop prevention (e.g., Observer applies exclude_observer_self_events)
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (cl:CognitiveLayer {name: $layer_name})
                MATCH (ef:EventFilter {name: $filter_name})
                MERGE (cl)-[:APPLIES_FILTER]->(ef)
                RETURN count(*) as count
            """, {
                'filter_name': filter_name,
                'layer_name': layer_name
            })

            record = result.single()
            success = record['count'] > 0 if record else False

            if success:
                logger.info(f"Linked filter '{filter_name}' to layer '{layer_name}'")
            else:
                logger.error(f"Failed to link filter '{filter_name}' to layer '{layer_name}'")

            return success

    def update_event_type_status(self, event_type_name: str, is_active: bool) -> bool:
        """
        Activate or deactivate an event type.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $name})
                SET et.is_active = $is_active,
                    et.updated_at = datetime()
                RETURN count(*) as count
            """, {
                'name': event_type_name,
                'is_active': is_active
            })

            record = result.single()
            success = record['count'] > 0 if record else False

            if success:
                status = "activated" if is_active else "deactivated"
                logger.info(f"{status.capitalize()} event type: {event_type_name}")

            return success

    def bulk_create_event_types(self, definitions: List[EventTypeDefinition]) -> Dict[str, Any]:
        """
        Create multiple event types in a single transaction.
        More efficient than creating them one-by-one.

        Returns:
            {
                "created": 5,
                "failed": 0,
                "event_types": [...]
            }
        """
        created = []
        failed = []

        with self.driver.session() as session:
            for definition in definitions:
                try:
                    # Use a transaction for each to isolate failures
                    with session.begin_transaction() as tx:
                        result = tx.run("""
                            MERGE (et:EventType {name: $name})
                            SET et.description = $description,
                                et.schema_version = $schema_version,
                                et.is_active = $is_active,
                                et.updated_at = datetime()

                            WITH et
                            MATCH (ss:SourceSystem {name: $source_system})
                            MERGE (et)-[:BELONGS_TO]->(ss)

                            WITH et
                            MATCH (ec:EventCategory {name: $category})
                            MERGE (et)-[:CATEGORIZED_AS]->(ec)

                            WITH et
                            MATCH (p:Priority {level: $priority})
                            MERGE (et)-[:HAS_PRIORITY]->(p)

                            WITH et
                            MATCH (rp:RetentionPolicy {name: $retention_policy})
                            MERGE (et)-[:GOVERNED_BY]->(rp)

                            RETURN et.name as name
                        """, {
                            'name': definition.name,
                            'description': definition.description,
                            'schema_version': definition.schema_version,
                            'is_active': definition.is_active,
                            'source_system': definition.source_system,
                            'category': definition.category,
                            'priority': definition.priority,
                            'retention_policy': definition.retention_policy
                        })

                        record = result.single()
                        if record:
                            created.append(definition.name)
                            tx.commit()
                        else:
                            failed.append(definition.name)

                except Exception as e:
                    logger.error(f"Failed to create event type {definition.name}: {e}")
                    failed.append(definition.name)

        logger.info(f"Bulk create: {len(created)} created, {len(failed)} failed")

        return {
            'created': len(created),
            'failed': len(failed),
            'event_types': created,
            'failed_event_types': failed
        }

    def add_cognitive_layer_consumer(self, event_type_name: str, layer_name: str) -> bool:
        """
        Add a cognitive layer as a consumer of an event type.

        Example:
            writer.add_cognitive_layer_consumer("slack.message", "Philosopher")
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $event_type_name})
                MATCH (cl:CognitiveLayer {name: $layer_name})
                MERGE (et)-[:CONSUMED_BY]->(cl)
                RETURN count(*) as count
            """, {
                'event_type_name': event_type_name,
                'layer_name': layer_name
            })

            record = result.single()
            success = record['count'] > 0 if record else False

            if success:
                logger.info(f"Added {layer_name} as consumer of {event_type_name}")

            return success

    def remove_cognitive_layer_consumer(self, event_type_name: str, layer_name: str) -> bool:
        """
        Remove a cognitive layer as a consumer of an event type.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (et:EventType {name: $event_type_name})
                      -[r:CONSUMED_BY]->(cl:CognitiveLayer {name: $layer_name})
                DELETE r
                RETURN count(*) as count
            """, {
                'event_type_name': event_type_name,
                'layer_name': layer_name
            })

            record = result.single()
            success = record['count'] > 0 if record else False

            if success:
                logger.info(f"Removed {layer_name} as consumer of {event_type_name}")

            return success

    def update_retention_policy(self, policy_name: str, retention_days: int,
                               action: str, archive_location: str = None) -> bool:
        """
        Update an existing retention policy dimension.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (rp:RetentionPolicy {name: $name})
                SET rp.retention_days = $retention_days,
                    rp.action = $action,
                    rp.archive_location = $archive_location,
                    rp.updated_at = datetime()
                RETURN count(*) as count
            """, {
                'name': policy_name,
                'retention_days': retention_days,
                'action': action,
                'archive_location': archive_location
            })

            record = result.single()
            success = record['count'] > 0 if record else False

            if success:
                logger.info(f"Updated retention policy: {policy_name}")

            return success

    def close(self):
        """Clean up Neo4j connection."""
        self.driver.close()
        logger.info("ObserverNeo4jWriter connection closed")


# Example usage
if __name__ == "__main__":
    print("\n✍️  Observer Neo4j Writer - Multi-dimensional Writes")
    print("=" * 80)

    writer = ObserverNeo4jWriter()

    # 1. Create a new event type with full dimensional context
    print("\n1️⃣ Creating GitHub Push Event Type:")
    github_push = EventTypeDefinition(
        name="github.push",
        description="Code pushed to GitHub repository",
        source_system="github",
        category="development",
        priority="medium",
        retention_policy="standard_90d",
        consumed_by_layers=["Observer", "Cartographer"],
        sample_payload={
            "repository": "basalmind",
            "branch": "main",
            "commits": []
        }
    )

    result = writer.create_event_type(github_push)
    if result:
        print(f"   ✅ Created: {result.get('name')}")
        print(f"   Priority: {result.get('priority')}")
        print(f"   Consumed by: {result.get('consumed_by')}")

    # 2. Create a source system
    print("\n2️⃣ Creating GitHub Source System:")
    github_system = SourceSystemDefinition(
        name="github",
        vendor="Microsoft",
        typical_event_volume="medium",
        ingestion_method="webhook",
        ingestion_frequency="real-time"
    )

    result = writer.create_source_system(github_system)
    if result:
        print(f"   ✅ Created: {result.get('name')}")
        print(f"   Vendor: {result.get('vendor')}")

    # 3. Create an event filter
    print("\n3️⃣ Creating Loop Prevention Filter:")
    result = writer.create_event_filter(
        name="exclude_observer_self_events",
        filter_type="loop_prevention",
        excluded_patterns=[
            "internal.observer_*",
            "internal.event_processing_*"
        ],
        reason="Prevent Observer from processing its own operational events"
    )

    if result:
        print(f"   ✅ Created filter: {result.get('name')}")
        print(f"   Type: {result.get('filter_type')}")

    # 4. Link filter to Observer layer
    print("\n4️⃣ Linking Filter to Observer Layer:")
    success = writer.link_filter_to_layer(
        "exclude_observer_self_events",
        "Observer"
    )
    print(f"   {'✅' if success else '❌'} Filter linked to Observer")

    # 5. Add cognitive layer consumer
    print("\n5️⃣ Adding Philosopher as Consumer:")
    success = writer.add_cognitive_layer_consumer(
        "github.push",
        "Philosopher"
    )
    print(f"   {'✅' if success else '❌'} Philosopher now consumes github.push")

    # 6. Deactivate event type
    print("\n6️⃣ Deactivating Event Type:")
    success = writer.update_event_type_status("github.push", False)
    print(f"   {'✅' if success else '❌'} github.push deactivated")

    writer.close()
    print("\n✅ Demo complete")
