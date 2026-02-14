"""
Neo4j Graph Writer - Phase 1

Creates semantic graph relationships from interpreted knowledge.
Uses namespace prefixing to avoid collisions with other BasalMind modules.

Handles entities from ALL sources: users, systems, IPs, conversations, etc.

Based on Claude Opus design specification.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from neo4j import GraphDatabase, Driver

logger = logging.getLogger(__name__)


class Neo4jWriter:
    """
    Write semantic graph nodes and relationships to Neo4j.
    
    Responsibilities:
    - Create semantic nodes with namespace prefix
    - Build relationships with lineage tracking
    - Support multi-source entities (Slack, nginx, Cloudflare, etc.)
    - Maintain temporal graph structure
    """
    
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
        namespace: str = "Interpreter"
    ):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.namespace = namespace  # Prefix for all node labels
        
        self.driver: Optional[Driver] = None
        
    def connect(self):
        """Initialize Neo4j driver."""
        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password)
        )
        # Test connection
        with self.driver.session(database=self.database) as session:
            result = session.run("RETURN 1 as test")
            result.single()
        logger.info(
            f"✅ Neo4j writer connected: {self.uri} "
            f"(namespace: {self.namespace})"
        )
        
    def close(self):
        """Close Neo4j driver."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j writer closed")
            
    def _label(self, entity_type: str) -> str:
        """Generate namespaced label."""
        return f"{self.namespace}_{entity_type}"
        
    def create_intent_node(
        self,
        intent_id: str,
        intent_type: str,
        text: str,
        confidence: float,
        actor_id: Optional[str],
        source_event_ids: List[str],
        timestamp: datetime
    ) -> str:
        """
        Create an Intent node with lineage.
        
        Args:
            intent_id: Unique intent identifier
            intent_type: Type of intent (asking_question, etc.)
            text: Intent text
            confidence: Confidence score (0.0-1.0)
            actor_id: User/IP/system that performed this intent
            source_event_ids: TimescaleDB event IDs this was extracted from
            timestamp: When the intent was expressed
            
        Returns:
            Node ID created
        """
        if not self.driver:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = f"""
            // Create Intent node
            MERGE (i:{self._label('Intent')} {{id: $intent_id}})
            SET i.type = $intent_type,
                i.text = $text,
                i.confidence = $confidence,
                i.extracted_at = datetime($timestamp)
                
            // Create/link Actor if provided
            WITH i
            {f'''
            MERGE (a:{self._label('Actor')} {{id: $actor_id}})
            MERGE (a)-[:{self._label('PERFORMED')}]->(i)
            ''' if actor_id else ''}
            
            // Create Event reference nodes for lineage
            WITH i
            UNWIND $source_event_ids as event_id
            MERGE (e:{self._label('Event')} {{id: event_id}})
            MERGE (i)-[:{self._label('EXTRACTED_FROM')} {{
                event_ids: $source_event_ids,
                extracted_at: datetime($timestamp)
            }}]->(e)
            
            RETURN i.id as node_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(
                query,
                intent_id=intent_id,
                intent_type=intent_type,
                text=text,
                confidence=confidence,
                actor_id=actor_id,
                source_event_ids=source_event_ids,
                timestamp=timestamp if isinstance(timestamp, str) else timestamp.isoformat()
            )
            record = result.single()
            
        logger.debug(f"📊 Created Intent node: {intent_id}")
        return record["node_id"] if record else intent_id
        
    def create_or_update_user(
        self,
        user_id: str,
        source_system: str,
        first_seen: datetime,
        last_active: datetime,
        interaction_count: int = 1
    ) -> str:
        """
        Create or update a User/Actor node.
        
        Works for:
        - Slack users
        - API users
        - Any identified actor
        
        Args:
            user_id: Unique user identifier
            source_system: Where this user was observed (slack, api, etc.)
            first_seen: First observation time
            last_active: Most recent activity
            interaction_count: Number of interactions
            
        Returns:
            Node ID
        """
        if not self.driver:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = f"""
            MERGE (u:{self._label('User')} {{id: $user_id}})
            ON CREATE SET
                u.first_seen = datetime($first_seen),
                u.source_systems = [$source_system],
                u.interaction_count = $interaction_count,
                u.last_active = datetime($last_active)
            ON MATCH SET
                u.source_systems = CASE
                    WHEN NOT $source_system IN u.source_systems
                    THEN u.source_systems + $source_system
                    ELSE u.source_systems
                END,
                u.interaction_count = u.interaction_count + $interaction_count,
                u.last_active = datetime($last_active)
            RETURN u.id as node_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(
                query,
                user_id=user_id,
                source_system=source_system,
                first_seen=first_seen.isoformat(),
                last_active=last_active.isoformat(),
                interaction_count=interaction_count
            )
            record = result.single()
            
        logger.debug(f"📊 Upserted User node: {user_id}")
        return record["node_id"] if record else user_id
        
    def create_system_node(
        self,
        system_id: str,
        system_type: str,
        first_seen: datetime
    ) -> str:
        """
        Create a System node (nginx, Cloudflare, database, etc.).
        
        Args:
            system_id: System identifier (e.g., "nginx", "cloudflare", "postgres")
            system_type: Type classification (webserver, cdn, database, service)
            first_seen: When we first observed this system
            
        Returns:
            Node ID
        """
        if not self.driver:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = f"""
            MERGE (s:{self._label('System')} {{id: $system_id}})
            ON CREATE SET
                s.type = $system_type,
                s.first_seen = datetime($first_seen)
            RETURN s.id as node_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(
                query,
                system_id=system_id,
                system_type=system_type,
                first_seen=first_seen.isoformat()
            )
            record = result.single()
            
        logger.debug(f"📊 Created System node: {system_id}")
        return record["node_id"] if record else system_id
        
    def create_thread_node(
        self,
        thread_id: str,
        topic: Optional[str],
        phase: Optional[str],
        started_at: datetime,
        last_activity: datetime,
        source_event_ids: List[str]
    ) -> str:
        """
        Create a Thread/Conversation node.
        
        Threads can represent:
        - Slack conversation threads
        - nginx request sequences (same session)
        - Cloudflare traffic patterns
        - Any conversation-like sequence
        
        Args:
            thread_id: Unique thread identifier
            topic: Extracted topic (optional)
            phase: initiation|exploration|discussion|resolution
            started_at: Thread start time
            last_activity: Last activity time
            source_event_ids: Event IDs in this thread
            
        Returns:
            Node ID
        """
        if not self.driver:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = f"""
            MERGE (t:{self._label('Thread')} {{id: $thread_id}})
            SET t.topic = $topic,
                t.phase = $phase,
                t.started_at = datetime($started_at),
                t.last_activity = datetime($last_activity)
                
            // Link to Event nodes for lineage
            WITH t
            UNWIND $source_event_ids as event_id
            MERGE (e:{self._label('Event')} {{id: event_id}})
            MERGE (t)-[:{self._label('CONTAINS')}]->(e)
            
            RETURN t.id as node_id
        """
        
        with self.driver.session(database=self.database) as session:
            result = session.run(
                query,
                thread_id=thread_id,
                topic=topic,
                phase=phase,
                started_at=started_at.isoformat(),
                last_activity=last_activity.isoformat(),
                source_event_ids=source_event_ids
            )
            record = result.single()
            
        logger.debug(f"📊 Created Thread node: {thread_id}")
        return record["node_id"] if record else thread_id
        
    def create_correlation_relationship(
        self,
        entity1_id: str,
        entity1_type: str,
        entity2_id: str,
        entity2_type: str,
        correlation_type: str,
        confidence: float,
        evidence_event_ids: List[str],
        detected_at: datetime
    ):
        """
        Create a correlation relationship between entities.
        
        Examples:
        - User U123 (Slack) CORRELATES_TO IP 192.168.1.100 (nginx)
        - Session S456 (Slack) CORRELATES_TO RequestSequence R789 (nginx)
        - Latency spike (nginx) CORRELATES_TO High volume (Cloudflare)
        
        Args:
            entity1_id: First entity ID
            entity1_type: First entity type (User, Session, etc.)
            entity2_id: Second entity ID
            entity2_type: Second entity type
            correlation_type: Type of correlation (temporal, spatial, causal)
            confidence: Correlation confidence (0.0-1.0)
            evidence_event_ids: Events supporting this correlation
            detected_at: When correlation was detected
        """
        if not self.driver:
            raise RuntimeError("Writer not connected. Call connect() first.")
            
        query = f"""
            MATCH (e1:{self._label(entity1_type)} {{id: $entity1_id}})
            MATCH (e2:{self._label(entity2_type)} {{id: $entity2_id}})
            MERGE (e1)-[r:{self._label('CORRELATES_WITH')} {{
                type: $correlation_type
            }}]->(e2)
            SET r.confidence = $confidence,
                r.evidence_event_ids = $evidence_event_ids,
                r.detected_at = datetime($detected_at)
        """
        
        with self.driver.session(database=self.database) as session:
            session.run(
                query,
                entity1_id=entity1_id,
                entity2_id=entity2_id,
                correlation_type=correlation_type,
                confidence=confidence,
                evidence_event_ids=evidence_event_ids,
                detected_at=detected_at.isoformat()
            )
            
        logger.debug(
            f"📊 Created correlation: {entity1_type}({entity1_id}) "
            f"<-{correlation_type}-> {entity2_type}({entity2_id})"
        )


# Example usage
def test_writer():
    """Test the Neo4jWriter."""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    writer = Neo4jWriter(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD"),
        database=os.getenv("NEO4J_DATABASE", "neo4j"),
        namespace=os.getenv("NEO4J_NAMESPACE", "Interpreter")
    )
    
    writer.connect()
    
    try:
        # Create a user node
        user_id = writer.create_or_update_user(
            user_id="U123",
            source_system="slack",
            first_seen=datetime.utcnow(),
            last_active=datetime.utcnow(),
            interaction_count=1
        )
        print(f"✅ Created user: {user_id}")
        
        # Create an intent node
        intent_id = writer.create_intent_node(
            intent_id="intent_001",
            intent_type="asking_question",
            text="How do we configure authentication?",
            confidence=0.85,
            actor_id="U123",
            source_event_ids=["evt-1", "evt-2"],
            timestamp=datetime.utcnow()
        )
        print(f"✅ Created intent: {intent_id}")
        
        # Create a system node
        system_id = writer.create_system_node(
            system_id="nginx",
            system_type="webserver",
            first_seen=datetime.utcnow()
        )
        print(f"✅ Created system: {system_id}")
        
    finally:
        writer.close()


if __name__ == "__main__":
    test_writer()
