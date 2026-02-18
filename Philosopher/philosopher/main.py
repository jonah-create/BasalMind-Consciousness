"""
Philosopher - Pattern Insight Generator (Stage 6: Real data analysis)

Responsibility: Generate insights from system observations.
Philosophy: Observe patterns, don't interfere. Recommend, don't command.

Stage 6 upgrade:
- Queries Interpreter's PostgreSQL for real intent/event history
- Queries Basal's advisory audit tables for decision patterns
- GPT-4o-mini synthesizes narrative insights from real data
- Background batch loop now runs actual analysis queries
- Template fallback when DB or LLM unavailable

Port: 5008 (HTTP health) + batch processor
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("philosopher")

# ── Health state ───────────────────────────────────────────────────────────────

class _HealthState:
    status: str = "starting"
    batches_processed: int = 0
    insights_generated: int = 0
    last_analysis_at: Optional[str] = None
    last_error: Optional[str] = None
    consultations: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
    pg_available: bool = False
    # Cache of last batch analysis for fast consult responses
    cached_patterns: Dict[str, Any] = {}

_health = _HealthState()

# ── PostgreSQL connection ──────────────────────────────────────────────────────

_pg_conn = None
_pg_lock = threading.Lock()

def _get_pg():
    global _pg_conn
    with _pg_lock:
        if _pg_conn is None or _pg_conn.closed:
            try:
                import psycopg2
                _pg_conn = psycopg2.connect(
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", 5432)),
                    dbname=os.getenv("POSTGRES_DB", "basalmind_interpreter_postgresql"),
                    user=os.getenv("POSTGRES_USER", "basalmind"),
                    password=os.getenv("POSTGRES_PASSWORD", "basalmind_secure_2024"),
                    connect_timeout=3,
                )
                _pg_conn.autocommit = True
                _health.pg_available = True
                logger.info("✅ Philosopher PostgreSQL connected")
            except Exception as e:
                logger.warning(f"PostgreSQL unavailable: {e}")
                _health.pg_available = False
                return None
    return _pg_conn


# ── OpenAI client ──────────────────────────────────────────────────────────────

_openai_client = None

def _get_openai():
    global _openai_client
    if _openai_client is None:
        try:
            from openai import OpenAI
            _openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        except Exception as e:
            logger.warning(f"OpenAI init failed: {e}")
    return _openai_client


# ── Real data queries ──────────────────────────────────────────────────────────

def _query_decision_patterns() -> Dict[str, Any]:
    """Query Basal's advisory audit tables for decision pattern data."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Intent distribution over last 24h
            cur.execute("""
                SELECT intent, action_type, COUNT(*) AS count,
                       ROUND(AVG(confidence)::numeric, 3) AS avg_confidence,
                       COUNT(*) FILTER (WHERE escalated = TRUE) AS escalated
                FROM basal_decisions
                WHERE recorded_at > NOW() - INTERVAL '24 hours'
                GROUP BY intent, action_type
                ORDER BY count DESC
                LIMIT 10
            """)
            intent_dist = [dict(r) for r in cur.fetchall()]

            # Overall stats
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE escalated = TRUE) AS total_escalated,
                       COUNT(*) FILTER (WHERE blocked = TRUE) AS total_blocked,
                       ROUND(AVG(confidence)::numeric, 3) AS avg_confidence
                FROM basal_decisions
                WHERE recorded_at > NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()
            stats = dict(row) if row else {}

            # Most active entities (which advisors were consulted most)
            cur.execute("""
                SELECT entity, COUNT(*) AS consultations,
                       ROUND(AVG(duration_ms)::numeric, 1) AS avg_ms,
                       COUNT(*) FILTER (WHERE influenced_decision = TRUE) AS influenced
                FROM basal_advisory_exchanges
                WHERE recorded_at > NOW() - INTERVAL '24 hours'
                GROUP BY entity
                ORDER BY consultations DESC
            """)
            entity_activity = [dict(r) for r in cur.fetchall()]

            return {
                "intent_distribution": intent_dist,
                "decision_stats": stats,
                "entity_activity": entity_activity,
                "window": "24h",
                "queried_at": datetime.utcnow().isoformat(),
            }
    except Exception as e:
        logger.warning(f"Decision pattern query failed: {e}")
        return {}


def _query_interpreter_patterns() -> Dict[str, Any]:
    """Query Interpreter's interpretations table for semantic patterns."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Recent interpretations summary
            cur.execute("""
                SELECT COUNT(*) AS total,
                       SUM(event_count) AS total_events,
                       ROUND(AVG(processing_duration_ms)::numeric, 1) AS avg_processing_ms
                FROM interpretations
                WHERE created_at > NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()
            interp_stats = dict(row) if row else {}

            # Check for anomalies flagged by Interpreter
            cur.execute("""
                SELECT COUNT(*) AS interpretations_with_anomalies
                FROM interpretations
                WHERE created_at > NOW() - INTERVAL '24 hours'
                  AND jsonb_array_length(anomalies) > 0
            """)
            row = cur.fetchone()
            anomaly_count = row[0] if row else 0

            return {
                "interpretation_stats": interp_stats,
                "anomaly_count_24h": anomaly_count,
                "queried_at": datetime.utcnow().isoformat(),
            }
    except Exception as e:
        logger.warning(f"Interpreter pattern query failed: {e}")
        return {}


# ── LLM insight synthesis ──────────────────────────────────────────────────────

PHILOSOPHER_SYSTEM = """You are Philosopher, a pattern insight generator for an AI engineering platform.

Your job is to synthesize narrative insights from system data.
You observe and recommend — you do not command, block, or execute anything.
Your insights are advisory inputs to Basal, which makes all decisions.

Given real system data, generate actionable insights in JSON:
{
  "insights": ["insight 1", "insight 2", "insight 3"],
  "anomaly_detected": true|false,
  "anomaly_description": "description if anomaly_detected, else null",
  "trend": "improving|stable|degrading|insufficient_data",
  "recommendations": ["recommendation 1", "recommendation 2"]
}

Keep insights specific, data-grounded, and actionable.
Do not fabricate data not present in the input.
Return ONLY the JSON object."""


def _llm_insights(intent: str, text: str, patterns: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """LLM insight synthesis from real pattern data. Returns None on failure."""
    client = _get_openai()
    if not client:
        return None

    _health.llm_calls += 1
    try:
        data_summary = json.dumps(patterns, indent=2, default=str)[:1500]
        prompt = f"""Current request context:
Intent: {intent}
Text: {text[:400] if text else "(no text)"}

Recent system patterns (real data):
{data_summary}

Generate philosophical insights about patterns, trends, and recommendations."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": PHILOSOPHER_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            max_tokens=400,
            temperature=0.4,
            timeout=8.0,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)
        _health.insights_generated += len(parsed.get("insights", []))

        return {
            "insights": parsed.get("insights", [])[:5],
            "anomaly_detected": bool(parsed.get("anomaly_detected", False)),
            "anomaly_description": parsed.get("anomaly_description"),
            "trend": parsed.get("trend", "insufficient_data"),
            "recommendations": parsed.get("recommendations", [])[:3],
            "generated_by": "llm",
        }

    except Exception as e:
        _health.llm_failures += 1
        logger.warning(f"[{correlation_id}] LLM insight synthesis failed ({type(e).__name__}): {e}")
        return None


def _template_insights(intent: str) -> Dict[str, Any]:
    """Template fallback insights."""
    _health.insights_generated += 1
    insight_map = {
        "system.alert": {
            "insights": ["Alert pattern detected — monitoring for recurrence", "Consider investigating root cause before escalation"],
            "trend": "degrading",
        },
        "request.feature": {
            "insights": ["Feature request volume observed", "Pattern: user-driven feature expansion"],
            "trend": "stable",
        },
        "request.bug_fix": {
            "insights": ["Defect pattern observed — tracking for systemic issues"],
            "trend": "stable",
        },
        "request.deployment": {
            "insights": ["Deployment request — Sentinel governance applied", "High-risk operation flagged for human review"],
            "trend": "stable",
        },
    }
    base = insight_map.get(intent, {
        "insights": [f"Observing intent pattern: {intent}"],
        "trend": "insufficient_data",
    })
    return {
        "insights": base["insights"],
        "anomaly_detected": False,
        "anomaly_description": None,
        "trend": base["trend"],
        "recommendations": ["Monitor for pattern recurrence", "Build baseline before drawing conclusions"],
        "generated_by": "template",
    }


# ── Background batch analysis ──────────────────────────────────────────────────

def _run_batch_analysis():
    """Background loop: query real data and cache patterns for fast consult."""
    poll_interval = int(os.getenv("PHILOSOPHER_POLL_INTERVAL", 120))  # 2 min
    time.sleep(10)  # Initial delay — let DB connections settle

    while True:
        try:
            decision_patterns = _query_decision_patterns()
            interpreter_patterns = _query_interpreter_patterns()

            combined = {**decision_patterns, **interpreter_patterns}
            if combined:
                _health.cached_patterns = combined

            _health.batches_processed += 1
            _health.last_analysis_at = datetime.utcnow().isoformat()
            _health.status = "ok"

            # Log summary
            total = combined.get("decision_stats", {}).get("total", 0)
            escalated = combined.get("decision_stats", {}).get("total_escalated", 0)
            logger.info(
                f"Philosopher batch {_health.batches_processed}: "
                f"{total} decisions (24h), {escalated} escalated"
            )

        except Exception as e:
            _health.last_error = str(e)
            logger.error(f"Batch analysis error: {e}")

        time.sleep(poll_interval)


# ── HTTP handler ───────────────────────────────────────────────────────────────

def _handle_consult(body: Dict[str, Any]) -> Dict[str, Any]:
    """Handle consultation — combine real patterns with LLM synthesis."""
    _health.consultations += 1
    intent = body.get("intent", "unknown")
    correlation_id = body.get("correlation_id", str(uuid.uuid4())[:8])

    context = body.get("context", {})
    text = context.get("text", "") or ""

    logger.info(f"[{correlation_id}] Philosophical analysis: intent={intent}")

    # Merge cached batch patterns with a quick fresh query for the consult
    patterns = dict(_health.cached_patterns)
    if not patterns:
        # No cache yet — do a quick live query
        decision_patterns = _query_decision_patterns()
        patterns = decision_patterns

    # Try LLM synthesis, fall back to template
    result = _llm_insights(intent, text, patterns, correlation_id)
    if result is None:
        result = _template_insights(intent)
        logger.debug(f"[{correlation_id}] Using template fallback")

    return {
        "entity": "Philosopher",
        "status": "analyzed",
        "correlation_id": correlation_id,
        "insights": result["insights"],
        "anomaly_detected": result["anomaly_detected"],
        "anomaly_description": result.get("anomaly_description"),
        "patterns": {
            "intent_frequency": patterns.get("intent_distribution", "unknown (building baseline)"),
            "anomaly_detected": result["anomaly_detected"],
            "trend": result["trend"],
            "decision_stats_24h": patterns.get("decision_stats", {}),
        },
        "recommendations": result["recommendations"],
        "data_source": "postgresql" if _health.pg_available else "template_only",
        "generated_by": result["generated_by"],
        "processed_at": datetime.utcnow().isoformat(),
    }


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            body = json.dumps({
                "status": _health.status,
                "service": "Philosopher",
                "version": "2.0.0",
                "stage": "6 — real data analysis",
                "pg_available": _health.pg_available,
                "llm_enabled": _get_openai() is not None,
                "batches_processed": _health.batches_processed,
                "insights_generated": _health.insights_generated,
                "last_analysis_at": _health.last_analysis_at,
                "consultations": _health.consultations,
                "llm_calls": _health.llm_calls,
                "llm_failures": _health.llm_failures,
                "timestamp": datetime.utcnow().isoformat(),
            }).encode()
            self.send_response(200 if _health.status == "ok" else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            body = json.dumps({
                "service": "Philosopher",
                "consultations": _health.consultations,
                "insights_generated": _health.insights_generated,
                "batches_processed": _health.batches_processed,
                "llm_calls": _health.llm_calls,
                "llm_failures": _health.llm_failures,
                "pg_available": _health.pg_available,
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/consult":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw)
                result = _handle_consult(data)
                resp = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(resp)
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default HTTP logging — use Python logger


def main():
    port = int(os.getenv("PHILOSOPHER_PORT", 5008))

    # Background analysis loop
    t = threading.Thread(target=_run_batch_analysis, daemon=True)
    t.start()
    logger.info("✅ Philosopher batch analysis thread started")

    # Initial DB connection attempt
    _get_pg()

    _health.status = "ok"
    server = HTTPServer(("0.0.0.0", port), _Handler)
    logger.info(f"✅ Philosopher HTTP server on port {port} (v2.0.0 — real data analysis)")
    server.serve_forever()


if __name__ == "__main__":
    main()
