"""
Event Correlator - Enriches events with correlation keys from Neo4j schema.

This module reads the correlation schema from Neo4j and adds the appropriate
correlation keys to events so they can be joined across data sources.

Architecture:
- Neo4j defines: SourceSystems, CorrelationKeys, CorrelationRules
- Observer calls: get_correlation_keys(event_data) during enrichment
- Returns: dict of correlation keys to add to event metadata
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class EventCorrelator:
    """
    Enriches events with correlation keys based on Neo4j schema.
    """

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._schema_cache = None
        self._cache_time = None
        self._cache_ttl = 300  # 5 minutes

    def _get_schema(self) -> Dict[str, Any]:
        """
        Get correlation schema from Neo4j (cached).

        Returns schema structure:
        {
            "sources": {
                "slack": {
                    "keys": [
                        {"name": "user_id", "required": True, "extraction_path": "normalized.user_id"},
                        {"name": "channel_id", "required": True, "extraction_path": "normalized.channel_id"}
                    ]
                }
            },
            "rules": [
                {
                    "name": "slack_to_webhook",
                    "description": "...",
                    "keys": ["timestamp", "request_path", "client_ip"]
                }
            ]
        }
        """
        # Check cache
        if self._schema_cache and self._cache_time:
            age = (datetime.utcnow() - self._cache_time).total_seconds()
            if age < self._cache_ttl:
                return self._schema_cache

        # Load from Neo4j
        with self.driver.session() as session:
            # Get source systems and their correlation keys
            result = session.run("""
                MATCH (s:SourceSystem)
                OPTIONAL MATCH (s)-[r:PROVIDES_KEY]->(k:CorrelationKey)
                RETURN s.name as source,
                       s.primary_key as primary_key,
                       collect({
                           name: k.name,
                           required: r.required,
                           extraction_path: r.extraction_path,
                           data_type: k.data_type
                       }) as keys
                ORDER BY source
            """)

            sources = {}
            for record in result:
                sources[record["source"]] = {
                    "primary_key": record["primary_key"],
                    "keys": [k for k in record["keys"] if k["name"]]  # Filter nulls
                }

            # Get correlation rules
            result = session.run("""
                MATCH (rule:CorrelationRule)
                OPTIONAL MATCH (rule)-[u:USES_KEY]->(k:CorrelationKey)
                RETURN rule.name as name,
                       rule.description as description,
                       rule.strategy as strategy,
                       collect({
                           key_name: k.name,
                           match_type: u.match_type,
                           window_seconds: u.window_seconds,
                           required_value: u.required_value,
                           propagate_to_slack: u.propagate_to_slack
                       }) as keys
            """)

            rules = []
            for record in result:
                rules.append({
                    "name": record["name"],
                    "description": record["description"],
                    "strategy": record["strategy"],
                    "keys": [k for k in record["keys"] if k["key_name"]]
                })

            schema = {
                "sources": sources,
                "rules": rules
            }

            self._schema_cache = schema
            self._cache_time = datetime.utcnow()

            logger.info(f"✅ Loaded correlation schema: {len(sources)} sources, {len(rules)} rules")
            return schema

    def extract_value(self, event_data: Dict[str, Any], path: str) -> Optional[Any]:
        """
        Extract a value from event_data using dot notation path.

        Example: "normalized.user_id" -> event_data["normalized"]["user_id"]
        """
        parts = path.split(".")
        current = event_data

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def enrich_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich event with correlation keys based on source system.

        Args:
            event_data: The event dict with source_system, normalized fields, etc.

        Returns:
            correlation_metadata: Dict to merge into event's metadata
        """
        source_system = event_data.get("source_system")
        if not source_system:
            logger.warning("Event missing source_system, cannot correlate")
            return {}

        # Get schema
        schema = self._get_schema()
        source_config = schema["sources"].get(source_system)

        if not source_config:
            logger.debug(f"No correlation config for source: {source_system}")
            return {}

        # Extract correlation keys
        correlation_keys = {}

        for key_config in source_config["keys"]:
            key_name = key_config["name"]
            extraction_path = key_config["extraction_path"]
            required = key_config.get("required", False)

            # Extract value
            value = self.extract_value(event_data, extraction_path)

            if value is not None:
                correlation_keys[key_name] = value
            elif required:
                logger.warning(
                    f"Required correlation key '{key_name}' missing from {source_system} event "
                    f"(path: {extraction_path})"
                )

        # Add correlation metadata
        result = {
            "correlation_keys": correlation_keys,
            "source_system": source_system,
            "correlatable": len(correlation_keys) > 0
        }

        # Apply correlation rules to find related events
        related_events = self._find_related_events(event_data, correlation_keys, schema)
        if related_events:
            result["related_event_ids"] = related_events

        logger.debug(
            f"Enriched {source_system} event with {len(correlation_keys)} correlation keys: "
            f"{list(correlation_keys.keys())}"
        )

        return result

    def _find_related_events(
        self,
        event_data: Dict[str, Any],
        correlation_keys: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> List[str]:
        """
        Find related event IDs based on correlation rules.

        This is a simple implementation - for now, just return empty list.
        In the future, this could query TimescaleDB to find matching events.
        """
        # TODO: Implement rule-based event correlation
        # For now, just ensure we have the keys needed for downstream correlation
        return []

    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()
            logger.info("Event correlator closed")


# Example usage:
"""
correlator = EventCorrelator(
    neo4j_uri="bolt://localhost:7687",
    neo4j_user="neo4j",
    neo4j_password="password"
)

event = {
    "source_system": "slack",
    "normalized": {
        "user_id": "U099BM321LH",
        "channel_id": "C0AA710CR7A"
    },
    "event_time": "2026-02-13T17:30:26Z"
}

correlation_metadata = correlator.enrich_event(event)
# Returns:
# {
#     "correlation_keys": {
#         "user_id": "U099BM321LH",
#         "channel_id": "C0AA710CR7A",
#         "timestamp": "2026-02-13T17:30:26Z"
#     },
#     "source_system": "slack",
#     "correlatable": True
# }

# Merge into event metadata
event["normalized"]["metadata"]["correlation"] = correlation_metadata
"""
