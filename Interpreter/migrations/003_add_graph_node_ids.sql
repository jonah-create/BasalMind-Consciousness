-- Migration 003: Add graph_node_ids linkage to interpretations
-- Links each interpretation record to the Neo4j nodes it created,
-- enabling Postgres → Neo4j traversal for forensic analysis and
-- Cartographer queries.
--
-- Safe: non-destructive, existing rows get NULL (expected).
-- Applied: 2026-02-17

ALTER TABLE interpretations
    ADD COLUMN IF NOT EXISTS graph_node_ids TEXT[];

COMMENT ON COLUMN interpretations.graph_node_ids IS
    'Neo4j node IDs (User, Intent) created during this interpretation batch. '
    'Enables Postgres→Neo4j lineage traversal without starting from the graph.';
