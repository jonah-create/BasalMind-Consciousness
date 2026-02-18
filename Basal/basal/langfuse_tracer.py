"""
Basal Langfuse Tracer (Langfuse SDK v3)

Every orchestration gets a Langfuse trace.
The reasoning chain maps directly to spans.
Advisory exchanges are captured as observations.

Trace structure per event:
  Trace root span: basal.orchestration (wraps full pipeline)
    Span: intent_classification
    Span: advisory.{entity}          (one per entity consulted)
    Span: reasoning.{step}           (one per reasoning step)
    Span: mcp.{skill}                (if a skill was invoked)
    Score: decision_confidence       (final decision confidence)

This gives full observability into:
  - Why Basal made each decision
  - Which advisories it received
  - How long each step took
  - Confidence calibration over time
"""

import logging
from typing import Any, Dict, List, Optional

from basal.config import config

logger = logging.getLogger("basal.langfuse")


class BasalTracer:
    """
    Wraps Langfuse v3 to provide structured trace instrumentation for Basal.

    In v3, traces are built via:
      - lf.create_trace_id() — generate a stable trace ID
      - TraceContext(trace_id=...) — anchor subsequent spans to that trace
      - lf.start_span(trace_context=ctx, ...) — create child spans
      - span.end() — close each span
      - lf.create_score(trace_id=...) — attach scores
      - lf.flush() — send buffered data

    Gracefully disabled when Langfuse is unavailable — never blocks orchestration.
    """

    def __init__(self):
        self._lf = None
        self._TraceContext = None
        self._enabled = False
        self._traces_created = 0
        self._connect()

    def _connect(self):
        """Initialize Langfuse client. Non-fatal if unavailable."""
        try:
            from langfuse import Langfuse
            from langfuse.types import TraceContext
            self._lf = Langfuse(
                host=config.LANGFUSE_HOST,
                public_key=config.LANGFUSE_PUBLIC_KEY,
                secret_key=config.LANGFUSE_SECRET_KEY,
            )
            self._lf.auth_check()
            self._TraceContext = TraceContext
            self._enabled = True
            logger.info(f"✅ Langfuse tracer connected: {config.LANGFUSE_HOST}")
        except Exception as e:
            logger.warning(f"⚠️ Langfuse unavailable (tracing disabled): {e}")
            self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    # ── Trace lifecycle ────────────────────────────────────────────────────────

    def start_trace(
        self,
        request_id: str,
        event_type: str,
        source: str,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Open a trace for one orchestration request.

        Returns a trace context dict with trace_id and root span,
        or None if Langfuse is disabled.
        """
        if not self._enabled:
            return None
        try:
            trace_id = self._lf.create_trace_id()
            ctx = self._TraceContext(trace_id=trace_id)

            # Root span anchors the whole orchestration pipeline
            root_span = self._lf.start_span(
                trace_context=ctx,
                name="basal.orchestration",
                input={
                    "event_type": event_type,
                    "source": source,
                    "request_id": request_id,
                },
                metadata={
                    "entity": "Basal",
                    "version": config.VERSION,
                    "request_id": request_id,
                    "session_id": session_id,
                    "user_id": user_id,
                    "source": source,
                },
            )

            self._traces_created += 1
            logger.debug(f"[LANGFUSE] Trace started: {trace_id} for request {request_id}")

            return {
                "trace_id": trace_id,
                "ctx": ctx,
                "root_span": root_span,
                "request_id": request_id,
            }
        except Exception as e:
            logger.warning(f"[LANGFUSE] Failed to start trace {request_id}: {e}")
            return None

    def end_trace(
        self,
        trace: Optional[Dict[str, Any]],
        decision: Dict[str, Any],
        request_id: str,
    ):
        """Close trace with final decision output and confidence score."""
        if not trace or not self._enabled:
            return
        try:
            trace_id = trace["trace_id"]
            root_span = trace["root_span"]

            # Update root span with final output
            root_span.update(
                output={
                    "action_type": decision.get("action_type"),
                    "skill": decision.get("skill"),
                    "blocked": decision.get("blocked"),
                    "escalate": decision.get("escalate"),
                    "rationale": decision.get("rationale"),
                    "confidence": decision.get("confidence"),
                    "reasoning_steps": len(decision.get("reasoning_trace", [])),
                },
            )
            root_span.end()

            # Score: decision confidence attached to the trace
            self._lf.create_score(
                trace_id=trace_id,
                name="decision_confidence",
                value=float(decision.get("confidence", 0.0)),
                comment=f"action_type={decision.get('action_type')} blocked={decision.get('blocked')}",
            )

            # Flush to ensure delivery
            self._lf.flush()
            logger.debug(f"[LANGFUSE] Trace ended and flushed: {trace_id}")
        except Exception as e:
            logger.warning(f"[LANGFUSE] Failed to end trace {request_id}: {e}")

    # ── Span creation ──────────────────────────────────────────────────────────

    def span_intent(
        self,
        trace: Optional[Dict[str, Any]],
        intent: str,
        confidence: float,
        priority: str,
        text_preview: str,
        duration_ms: float,
    ):
        """Span for intent classification step."""
        if not trace or not self._enabled:
            return
        try:
            root_span = trace["root_span"]
            span = root_span.start_span(
                name="intent_classification",
                input={"text_preview": text_preview[:200]},
                output={"intent": intent, "confidence": confidence, "priority": priority},
                metadata={"duration_ms": duration_ms, "mode": "rule_based"},
            )
            span.end()
        except Exception as e:
            logger.warning(f"[LANGFUSE] intent span failed: {e}")

    def span_advisory(
        self,
        trace: Optional[Dict[str, Any]],
        entity: str,
        request: Dict[str, Any],
        response: Dict[str, Any],
        duration_ms: float,
        available: bool,
    ):
        """Span for one advisory exchange."""
        if not trace or not self._enabled:
            return
        try:
            root_span = trace["root_span"]
            span = root_span.start_span(
                name=f"advisory.{entity}",
                input={
                    "intent": request.get("intent"),
                    "context": request.get("context", {}),
                },
                output={
                    "status": response.get("status"),
                    "entity": response.get("entity"),
                    "summary": _extract_advisory_summary(entity, response),
                },
                metadata={
                    "duration_ms": duration_ms,
                    "available": available,
                    "entity": entity,
                },
                level="DEFAULT" if available else "WARNING",
            )
            span.end()
        except Exception as e:
            logger.warning(f"[LANGFUSE] advisory span {entity} failed: {e}")

    def span_reasoning(
        self,
        trace: Optional[Dict[str, Any]],
        reasoning_trace: List[Dict[str, Any]],
        final_decision: Dict[str, Any],
    ):
        """Span for the full reasoning chain — one span per reasoning step."""
        if not trace or not self._enabled:
            return
        try:
            root_span = trace["root_span"]
            for step in reasoning_trace:
                span = root_span.start_span(
                    name=f"reasoning.{step['step']}",
                    input=step.get("signals", {}),
                    output={"conclusion": step.get("conclusion")},
                    metadata={"confidence": step.get("confidence", 0.0)},
                )
                span.end()
        except Exception as e:
            logger.warning(f"[LANGFUSE] reasoning span failed: {e}")

    def span_mcp(
        self,
        trace: Optional[Dict[str, Any]],
        skill: str,
        arguments: Dict[str, Any],
        success: bool,
        duration_ms: float,
        content_preview: Optional[str] = None,
        error: Optional[str] = None,
    ):
        """Span for MCP skill invocation."""
        if not trace or not self._enabled:
            return
        try:
            root_span = trace["root_span"]
            span = root_span.start_span(
                name=f"mcp.{skill}",
                input={"skill": skill, "arguments": arguments},
                output={
                    "success": success,
                    "content_preview": (content_preview or "")[:300],
                    "error": error,
                },
                metadata={"duration_ms": duration_ms},
                level="DEFAULT" if success else "ERROR",
            )
            span.end()
        except Exception as e:
            logger.warning(f"[LANGFUSE] MCP span failed: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "traces_created": self._traces_created,
            "host": config.LANGFUSE_HOST,
        }


# ── Utilities ──────────────────────────────────────────────────────────────────

def _extract_advisory_summary(entity: str, response: Dict[str, Any]) -> Optional[str]:
    """Extract the key advisory signal from each entity's response."""
    if entity == "sentinel":
        risk = response.get("risk_assessment", {})
        return f"decision={response.get('decision')} risk={risk.get('score')}"
    elif entity == "cartographer":
        story = response.get("story", {})
        return f"story={story.get('story_id')} priority={story.get('priority')}"
    elif entity == "assembler":
        plan = response.get("plan", {})
        return f"plan={plan.get('plan_id')} days={plan.get('estimated_days')}"
    elif entity == "philosopher":
        return f"insights={len(response.get('insights', []))} anomaly={response.get('patterns', {}).get('anomaly_detected')}"
    return str(response.get("status", "unknown"))
