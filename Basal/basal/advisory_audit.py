"""
Basal Advisory Audit Store

System of record: PostgreSQL (basal_advisory_exchanges, basal_decisions)
Live cache:       Redis (24h TTL for fast lookup)
Learning queries: PostgreSQL analytical queries (see /audit/analytics endpoint)

Every advisory exchange persists two layers:
  1. PostgreSQL — permanent, queryable, enables learning over time
  2. Redis      — TTL-based, fast lookup for current session context

What you can learn from this:
  - Advisory utilization rate per entity (did the advice change the decision?)
  - Entity latency vs. advisory influence (is slow advice worth waiting for?)
  - Risk calibration (does Sentinel risk score predict actual outcomes?)
  - Intent→decision patterns (which intents always observe-only? fix the classifier)
  - Confidence calibration (does stated confidence match actual decision quality?)
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from basal.config import config

logger = logging.getLogger("basal.audit")

# Redis key prefixes (live cache)
ADVISORY_KEY = "basal:advisory:"
ADVISORY_INDEX_KEY = "basal:advisory:index:"
DECISION_KEY = "basal:decision:"
AUDIT_LIST_KEY = "basal:audit:recent"

ADVISORY_TTL = 86400      # 24h Redis TTL
RING_BUFFER_SIZE = 500    # In-memory fallback size


class AdvisoryAuditStore:
    """
    Dual-layer advisory audit:
      - PostgreSQL: durable system of record, analytical queries
      - Redis:      fast live lookup, ring buffer for recent exchanges
      - Memory:     ring buffer fallback when Redis unavailable
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._buffer: List[Dict[str, Any]] = []
        self._pg_conn = None
        self._pg_available = False
        self._connect_postgres()

    def _connect_postgres(self):
        """Connect to PostgreSQL (non-fatal if unavailable)."""
        try:
            self._pg_conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                connect_timeout=3,
            )
            self._pg_conn.autocommit = True
            self._pg_available = True
            logger.info("✅ Advisory audit PostgreSQL connected (durable store active)")
        except Exception as e:
            logger.warning(f"⚠️ Advisory audit PostgreSQL unavailable (Redis-only mode): {e}")
            self._pg_available = False

    def _ensure_pg(self) -> bool:
        """Ensure PostgreSQL connection is live; reconnect if dropped."""
        if not self._pg_available:
            return False
        try:
            self._pg_conn.cursor().execute("SELECT 1")
            return True
        except Exception:
            try:
                self._connect_postgres()
                return self._pg_available
            except Exception:
                return False

    # ── Record advisory exchange ───────────────────────────────────────────────

    def record_advisory(
        self,
        request_id: str,
        entity: str,
        advisory_request: Dict[str, Any],
        advisory_response: Dict[str, Any],
        duration_ms: float,
        available: bool,
        correlation_id: str,
    ) -> str:
        """Persist one advisory exchange to PostgreSQL + Redis + memory."""
        advisory_id = f"{request_id}:{entity}"
        record = {
            "advisory_id": advisory_id,
            "request_id": request_id,
            "entity": entity,
            "correlation_id": correlation_id,
            "available": available,
            "duration_ms": round(duration_ms, 2),
            "request": self._sanitize(advisory_request),
            "response": self._sanitize(advisory_response),
            "influenced_decision": None,
            "influence_note": None,
            "recorded_at": datetime.utcnow().isoformat(),
        }

        # Layer 1: PostgreSQL (durable system of record)
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO basal_advisory_exchanges
                            (advisory_id, request_id, entity, correlation_id,
                             available, duration_ms, request_json, response_json, recorded_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (advisory_id) DO NOTHING
                    """, (
                        advisory_id, request_id, entity, correlation_id,
                        available, duration_ms,
                        json.dumps(record["request"]),
                        json.dumps(record["response"]),
                    ))
            except Exception as e:
                logger.warning(f"[AUDIT] PostgreSQL write failed for {advisory_id}: {e}")

        # Layer 2: Redis (live cache, 24h TTL)
        if self._redis:
            try:
                self._redis.setex(
                    f"{ADVISORY_KEY}{advisory_id}",
                    ADVISORY_TTL,
                    json.dumps(record),
                )
                self._redis.rpush(f"{ADVISORY_INDEX_KEY}{request_id}", entity)
                self._redis.expire(f"{ADVISORY_INDEX_KEY}{request_id}", ADVISORY_TTL)
                self._redis.lpush(AUDIT_LIST_KEY, json.dumps({
                    "advisory_id": advisory_id,
                    "entity": entity,
                    "available": available,
                    "duration_ms": record["duration_ms"],
                    "recorded_at": record["recorded_at"],
                    "response_status": advisory_response.get("status", "unknown"),
                }))
                self._redis.ltrim(AUDIT_LIST_KEY, 0, RING_BUFFER_SIZE - 1)
            except Exception as e:
                logger.warning(f"[AUDIT] Redis write failed for {advisory_id}: {e}")

        # Layer 3: In-memory ring buffer
        self._buffer.append(record)
        if len(self._buffer) > RING_BUFFER_SIZE:
            self._buffer = self._buffer[-RING_BUFFER_SIZE:]

        logger.debug(f"[AUDIT] {advisory_id} recorded ({duration_ms:.0f}ms, available={available})")
        return advisory_id

    def record_decision(
        self,
        request_id: str,
        decision_dict: Dict[str, Any],
        intent: str,
        source: str,
        correlation_id: str,
    ):
        """Persist decision to PostgreSQL + Redis."""
        action_type = decision_dict.get("action_type", "unknown")
        skill = decision_dict.get("skill")
        confidence = decision_dict.get("confidence", 0.0)
        blocked = decision_dict.get("blocked", False)
        block_reason = decision_dict.get("block_reason")
        escalated = decision_dict.get("escalate", False)
        rationale = decision_dict.get("rationale", "")

        # PostgreSQL
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO basal_decisions
                            (request_id, correlation_id, intent, source,
                             action_type, skill, confidence, blocked, block_reason,
                             escalated, rationale, decision_json, recorded_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (request_id) DO NOTHING
                    """, (
                        request_id, correlation_id, intent, source,
                        action_type, skill, confidence, blocked, block_reason,
                        escalated, rationale, json.dumps(decision_dict),
                    ))
            except Exception as e:
                logger.warning(f"[AUDIT] PostgreSQL decision write failed {request_id}: {e}")

        # Redis
        if self._redis:
            try:
                record = {
                    "request_id": request_id,
                    "intent": intent,
                    "source": source,
                    "decision": decision_dict,
                    "recorded_at": datetime.utcnow().isoformat(),
                }
                self._redis.setex(
                    f"{DECISION_KEY}{request_id}",
                    ADVISORY_TTL,
                    json.dumps(record),
                )
            except Exception as e:
                logger.warning(f"[AUDIT] Redis decision write failed: {e}")

    def annotate_decision_influence(
        self,
        request_id: str,
        entity: str,
        influenced: bool,
        influence_note: str,
    ):
        """Update advisory record with whether it influenced the decision."""
        advisory_id = f"{request_id}:{entity}"

        # PostgreSQL
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("""
                        UPDATE basal_advisory_exchanges
                        SET influenced_decision = %s, influence_note = %s
                        WHERE advisory_id = %s
                    """, (influenced, influence_note, advisory_id))
            except Exception as e:
                logger.warning(f"[AUDIT] Influence annotation failed {advisory_id}: {e}")

        # Redis
        if self._redis:
            try:
                raw = self._redis.get(f"{ADVISORY_KEY}{advisory_id}")
                if raw:
                    record = json.loads(raw)
                    record["influenced_decision"] = influenced
                    record["influence_note"] = influence_note
                    self._redis.setex(
                        f"{ADVISORY_KEY}{advisory_id}",
                        ADVISORY_TTL,
                        json.dumps(record),
                    )
            except Exception:
                pass

    # ── Retrieval ──────────────────────────────────────────────────────────────

    def get_advisory(self, request_id: str, entity: str) -> Optional[Dict[str, Any]]:
        advisory_id = f"{request_id}:{entity}"
        # Redis first
        if self._redis:
            try:
                raw = self._redis.get(f"{ADVISORY_KEY}{advisory_id}")
                if raw:
                    return json.loads(raw)
            except Exception:
                pass
        # PostgreSQL fallback
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM basal_advisory_exchanges WHERE advisory_id = %s",
                        (advisory_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)
            except Exception:
                pass
        # Memory fallback
        for r in reversed(self._buffer):
            if r["advisory_id"] == advisory_id:
                return r
        return None

    def get_all_advisories_for_request(self, request_id: str) -> List[Dict[str, Any]]:
        # PostgreSQL (most reliable)
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM basal_advisory_exchanges WHERE request_id = %s ORDER BY recorded_at",
                        (request_id,)
                    )
                    return [dict(r) for r in cur.fetchall()]
            except Exception:
                pass
        # Redis fallback
        if self._redis:
            try:
                entities = self._redis.lrange(f"{ADVISORY_INDEX_KEY}{request_id}", 0, -1)
                return [r for e in entities if (r := self.get_advisory(request_id, e))]
            except Exception:
                pass
        return [r for r in self._buffer if r["request_id"] == request_id]

    def get_decision(self, request_id: str) -> Optional[Dict[str, Any]]:
        if self._redis:
            try:
                raw = self._redis.get(f"{DECISION_KEY}{request_id}")
                if raw:
                    return json.loads(raw)
            except Exception:
                pass
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM basal_decisions WHERE request_id = %s",
                        (request_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)
            except Exception:
                pass
        return None

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        if self._redis:
            try:
                entries = self._redis.lrange(AUDIT_LIST_KEY, 0, limit - 1)
                return [json.loads(e) for e in entries]
            except Exception:
                pass
        return [
            {
                "advisory_id": r["advisory_id"],
                "entity": r["entity"],
                "available": r["available"],
                "duration_ms": r["duration_ms"],
                "recorded_at": r["recorded_at"],
            }
            for r in reversed(self._buffer[-limit:])
        ]

    # ── Analytics (learning queries) ───────────────────────────────────────────

    def get_analytics(self) -> Dict[str, Any]:
        """
        Learning analytics from the advisory audit trail.

        Answers:
        - Advisory utilization rate per entity
        - Average latency per entity
        - Decision distribution by action_type
        - Intent-to-action mapping
        - Blocked/escalated rates
        """
        if not self._ensure_pg():
            return {"error": "PostgreSQL unavailable — analytics require durable store"}

        analytics: Dict[str, Any] = {}

        try:
            with self._pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

                # Entity utilization: how often advisory influenced decision
                cur.execute("""
                    SELECT
                        entity,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE available = TRUE)  AS available,
                        ROUND(AVG(duration_ms)::numeric, 1)       AS avg_latency_ms,
                        COUNT(*) FILTER (WHERE influenced_decision = TRUE)  AS influenced,
                        COUNT(*) FILTER (WHERE influenced_decision = FALSE) AS not_influenced,
                        ROUND(
                            100.0 * COUNT(*) FILTER (WHERE influenced_decision = TRUE)
                            / NULLIF(COUNT(*) FILTER (WHERE influenced_decision IS NOT NULL), 0),
                        1) AS influence_rate_pct
                    FROM basal_advisory_exchanges
                    GROUP BY entity
                    ORDER BY total DESC
                """)
                analytics["entity_utilization"] = [dict(r) for r in cur.fetchall()]

                # Decision distribution
                cur.execute("""
                    SELECT
                        action_type,
                        COUNT(*) AS count,
                        ROUND(AVG(confidence)::numeric, 3) AS avg_confidence,
                        COUNT(*) FILTER (WHERE blocked = TRUE)   AS blocked,
                        COUNT(*) FILTER (WHERE escalated = TRUE) AS escalated
                    FROM basal_decisions
                    GROUP BY action_type
                    ORDER BY count DESC
                """)
                analytics["decision_distribution"] = [dict(r) for r in cur.fetchall()]

                # Intent → action mapping
                cur.execute("""
                    SELECT
                        intent,
                        action_type,
                        COUNT(*) AS count,
                        ROUND(AVG(confidence)::numeric, 3) AS avg_confidence
                    FROM basal_decisions
                    GROUP BY intent, action_type
                    ORDER BY intent, count DESC
                """)
                analytics["intent_to_action"] = [dict(r) for r in cur.fetchall()]

                # Overall stats
                cur.execute("""
                    SELECT
                        COUNT(*) AS total_decisions,
                        COUNT(*) FILTER (WHERE blocked = TRUE)   AS total_blocked,
                        COUNT(*) FILTER (WHERE escalated = TRUE) AS total_escalated,
                        ROUND(AVG(confidence)::numeric, 3)       AS avg_confidence
                    FROM basal_decisions
                """)
                row = cur.fetchone()
                analytics["summary"] = dict(row) if row else {}

        except Exception as e:
            logger.error(f"[AUDIT] Analytics query failed: {e}")
            analytics["error"] = str(e)

        return analytics

    def get_stats(self) -> Dict[str, Any]:
        """Quick stats for health endpoint."""
        stats = {
            "buffer_size": len(self._buffer),
            "postgres_available": self._pg_available,
        }
        if self._ensure_pg():
            try:
                with self._pg_conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM basal_advisory_exchanges")
                    stats["total_advisory_exchanges"] = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM basal_decisions")
                    stats["total_decisions"] = cur.fetchone()[0]
            except Exception:
                pass
        return stats

    def _sanitize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(data, dict):
            return {}
        sanitized = {}
        for k, v in data.items():
            if k == "raw_payload":
                continue
            if isinstance(v, str) and len(v) > 1000:
                sanitized[k] = v[:1000] + "…[truncated]"
            elif isinstance(v, dict):
                sanitized[k] = self._sanitize(v)
            else:
                sanitized[k] = v
        return sanitized
