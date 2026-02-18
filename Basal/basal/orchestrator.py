"""
Basal Orchestrator - Core coordination engine.

Philosophy: Basal reasons. Entities inform. MCP executes.

Every advisory exchange is fully auditable:
  - What Basal asked each entity
  - What each entity returned
  - How those advisories influenced the decision
  - The complete reasoning trace
  - Which MCP skill was invoked and what it returned
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from basal.config import config
from basal.intent_classifier import IntentClassifier, IntentResult
from basal.redis_session import RedisSessionManager
from basal.logging_config import CorrelatedLogger
from basal.reasoning import (
    ReasoningEngine,
    AdvisorySet,
    Decision,
    parse_advisories,
)
from basal.advisory_audit import AdvisoryAuditStore
from basal.langfuse_tracer import BasalTracer

logger = logging.getLogger("basal.orchestrator")


class Orchestrator:
    """
    Basal's coordination engine.

    Pipeline per event:
      1. Classify intent
      2. Build structured advisory request
      3. Gather advisories (concurrent, audited, graceful degradation)
      4. Parse advisories into typed objects
      5. Reason → Decision (with trace)
      6. Annotate advisories with decision influence
      7. Execute via MCP (if decision calls for it)
      8. Persist full orchestration record
    """

    def __init__(
        self,
        session_manager: RedisSessionManager,
        intent_classifier: IntentClassifier,
        mcp_client=None,
        audit_store: Optional[AdvisoryAuditStore] = None,
        tracer: Optional[BasalTracer] = None,
        nats_publisher=None,
    ):
        self._sessions = session_manager
        self._classifier = intent_classifier
        self._mcp = mcp_client
        self._reasoner = ReasoningEngine()
        self._audit = audit_store
        self._tracer = tracer
        self._nats_publisher = nats_publisher  # DecisionPublisher for Conductor
        self._http_client: Optional[httpx.AsyncClient] = None
        self._events_processed = 0
        self._entity_availability: Dict[str, bool] = {
            "cartographer": False,
            "sentinel": False,
            "assembler": False,
            "philosopher": False,
        }

    async def initialize(self):
        self._http_client = httpx.AsyncClient(
            timeout=config.ENTITY_TIMEOUT_SECONDS,
            follow_redirects=True,
        )
        logger.info("✅ Orchestrator initialized (reasoning engine + advisory audit active)")

    async def shutdown(self):
        if self._http_client:
            await self._http_client.aclose()

    # ── Main event handler ─────────────────────────────────────────────────────

    async def handle_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process one event through the full auditable orchestration pipeline."""
        request_id = str(uuid.uuid4())[:8]
        log = CorrelatedLogger(correlation_id=request_id, entity="Orchestrator")

        event_type = event.get("event_type", "unknown")
        source = event.get("source_system", "unknown")
        log.info(f"Event: {event_type} from {source}", stage="RECEIVE")

        # Open Langfuse trace
        normalized = event.get("normalized", {})
        user_id = (normalized.get("user_id") if isinstance(normalized, dict) else None)
        lf_trace = self._tracer.start_trace(
            request_id=request_id,
            event_type=event_type,
            source=source,
            user_id=user_id,
        ) if self._tracer else None

        classify_start = datetime.utcnow()

        # 1. Classify intent
        try:
            intent: IntentResult = self._classifier.classify_event(event)
            log.info(
                f"Intent={intent.intent} conf={intent.confidence:.2f} priority={intent.priority}",
                stage="CLASSIFY",
            )
        except Exception as e:
            log.error(f"Classification failed: {e}", stage="CLASSIFY", exc_info=True)
            intent = IntentResult(intent="unknown", confidence=0.0)

        # Langfuse: intent span
        if self._tracer and lf_trace:
            text = (normalized.get("text", "") if isinstance(normalized, dict) else "") or ""
            classify_ms = (datetime.utcnow() - classify_start).total_seconds() * 1000
            self._tracer.span_intent(
                trace=lf_trace,
                intent=intent.intent,
                confidence=intent.confidence,
                priority=intent.priority,
                text_preview=text[:200],
                duration_ms=classify_ms,
            )

        # 2. Create session
        session_id = self._sessions.create_session({
            "request_id": request_id,
            "event_type": event_type,
            "source": source,
            "intent": intent.intent,
            "confidence": intent.confidence,
            "priority": intent.priority,
            "started_at": datetime.utcnow().isoformat(),
        })

        # 3+4. Gather and parse advisory inputs
        raw_consultations: Dict[str, Any] = {}
        if intent.requires_entities:
            log.info(f"Requesting advisories: {intent.requires_entities}", stage="ADVISE")
            raw_consultations = await self._gather_advisories(
                entities=intent.requires_entities,
                event=event,
                intent=intent,
                request_id=request_id,
                log=log,
                lf_trace=lf_trace,
            )

        advisories: AdvisorySet = parse_advisories(raw_consultations)

        # 5. Reason → Decision (with full trace)
        log.info("Reasoning over advisories", stage="REASON")
        decision: Decision = self._reasoner.reason(
            intent=intent,
            advisories=advisories,
            event=event,
        )
        log.info(
            f"Decision: action={decision.action_type} skill={decision.skill or 'none'} "
            f"conf={decision.confidence:.3f} blocked={decision.blocked}",
            stage="REASON",
        )

        # Langfuse: reasoning spans
        if self._tracer and lf_trace:
            self._tracer.span_reasoning(
                trace=lf_trace,
                reasoning_trace=decision.to_dict().get("reasoning_trace", []),
                final_decision=decision.to_dict(),
            )

        # 6. Annotate advisories with decision influence (closes the audit loop)
        if self._audit and raw_consultations:
            self._annotate_influence(request_id, advisories, decision)
            self._audit.record_decision(
                request_id=request_id,
                decision_dict=decision.to_dict(),
                intent=intent.intent,
                source=source,
                correlation_id=request_id,
            )

        # 7. Execute via MCP (only if decision calls for a skill)
        mcp_result = None
        if decision.skill and not decision.blocked and self._mcp:
            log.info(f"Executing MCP: {decision.skill}", stage="EXECUTE")
            mcp_result = await self._mcp.invoke(
                skill_name=decision.skill,
                arguments=decision.skill_args,
                correlation_id=request_id,
            )
            outcome = "succeeded" if mcp_result.success else f"failed: {mcp_result.error}"
            log.info(f"MCP {decision.skill} {outcome} ({mcp_result.duration_ms:.0f}ms)", stage="EXECUTE")

            # Langfuse: MCP span
            if self._tracer and lf_trace:
                self._tracer.span_mcp(
                    trace=lf_trace,
                    skill=decision.skill,
                    arguments=decision.skill_args,
                    success=mcp_result.success,
                    duration_ms=mcp_result.duration_ms,
                    content_preview=str(mcp_result.content)[:300] if mcp_result.content else None,
                    error=mcp_result.error,
                )

        # 8. Persist complete record
        record = {
            "request_id": request_id,
            "session_id": session_id,
            "event_type": event_type,
            "source": source,
            "intent": intent.intent,
            "intent_confidence": intent.confidence,
            "priority": intent.priority,
            "advisories_requested": list(raw_consultations.keys()),
            "decision": decision.to_dict(),
            "mcp_result": mcp_result.to_dict() if mcp_result else None,
            "processed_at": datetime.utcnow().isoformat(),
        }
        self._sessions.save_orchestration(request_id, record)

        # Langfuse: close trace with final decision
        if self._tracer and lf_trace:
            self._tracer.end_trace(
                trace=lf_trace,
                decision=decision.to_dict(),
                request_id=request_id,
            )

        # 9. Publish decision to NATS so Conductor can act on it
        # Subject: decisions.{channel_id} or decisions.internal for non-Slack events
        try:
            if self._nats_publisher:
                normalized_data = event.get("normalized", {}) or {}
                channel_id = (
                    normalized_data.get("channel_id")
                    if isinstance(normalized_data, dict) else None
                ) or "internal"
                decision_subject = f"decisions.{channel_id}"
                decision_payload = {
                    "request_id": request_id,
                    "channel_id": channel_id,
                    "event_type": event_type,
                    "source": source,
                    "intent": intent.intent,
                    "intent_confidence": intent.confidence,
                    "priority": intent.priority,
                    "decision": decision.to_dict(),
                    "event": event,
                    "published_at": datetime.utcnow().isoformat(),
                }
                await self._nats_publisher.publish_decision(decision_subject, decision_payload)
                log.info(f"Decision published → {decision_subject}", stage="PUBLISH")
        except Exception as e:
            log.warning(f"Decision publication failed (non-fatal): {e}", stage="PUBLISH")

        self._events_processed += 1
        log.info(f"Complete — total={self._events_processed}", stage="COMPLETE")
        return record

    # ── Advisory gathering (fully audited) ────────────────────────────────────

    async def _gather_advisories(
        self,
        entities: List[str],
        event: Dict[str, Any],
        intent: IntentResult,
        request_id: str,
        log: CorrelatedLogger,
        lf_trace: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Request advisory inputs concurrently.
        Each exchange is timed and recorded to the audit store and Langfuse.
        """
        advisory_request = self._build_advisory_request(event, intent, request_id)

        entity_urls = {
            "cartographer": config.CARTOGRAPHER_URL,
            "sentinel": config.SENTINEL_URL,
            "assembler": config.ASSEMBLER_URL,
            "philosopher": config.PHILOSOPHER_URL,
        }

        tasks = {
            entity: self._request_advisory(
                entity_name=entity,
                url=f"{entity_urls[entity]}/consult",
                request=advisory_request,
                request_id=request_id,
                log=log,
                lf_trace=lf_trace,
            )
            for entity in entities
            if entity in entity_urls
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        advisories: Dict[str, Any] = {}
        for entity, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                log.warning(f"Advisory from {entity} failed: {result}", stage="ADVISE")
                resp = {"status": "error", "error": str(result)}
                if self._audit:
                    self._audit.record_advisory(
                        request_id=request_id,
                        entity=entity,
                        advisory_request=advisory_request,
                        advisory_response=resp,
                        duration_ms=0.0,
                        available=False,
                        correlation_id=request_id,
                    )
                advisories[entity] = resp
            else:
                advisories[entity] = result

        return advisories

    def _build_advisory_request(
        self,
        event: Dict[str, Any],
        intent: IntentResult,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """
        Distilled advisory request — entities get context, not raw events.
        Basal prepares what each entity needs to give good advice.
        """
        normalized = event.get("normalized", {})
        text = (normalized.get("text", "") if isinstance(normalized, dict) else "") or ""
        user_id = (normalized.get("user_id", "unknown") if isinstance(normalized, dict) else "unknown")

        return {
            "intent": intent.intent,
            "confidence": intent.confidence,
            "priority": intent.priority,
            "correlation_id": correlation_id,
            "context": {
                "text": text[:500],
                "user_id": user_id,
                "event_type": event.get("event_type", "unknown"),
                "source_system": event.get("source_system", "unknown"),
                "observed_at": event.get("observed_at", datetime.utcnow().isoformat()),
            },
            "event": event,
        }

    async def _request_advisory(
        self,
        entity_name: str,
        url: str,
        request: Dict[str, Any],
        request_id: str,
        log: CorrelatedLogger,
        lf_trace: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Request one advisory, time it, and record it to audit store and Langfuse."""
        if not self._http_client:
            resp = {"status": "unavailable", "reason": "no_http_client"}
            if self._audit:
                self._audit.record_advisory(
                    request_id=request_id, entity=entity_name,
                    advisory_request=request, advisory_response=resp,
                    duration_ms=0.0, available=False, correlation_id=request_id,
                )
            return resp

        started = datetime.utcnow()
        try:
            http_resp = await self._http_client.post(url, json=request, timeout=config.ENTITY_TIMEOUT_SECONDS)
            duration_ms = (datetime.utcnow() - started).total_seconds() * 1000

            if http_resp.status_code == 200:
                self._entity_availability[entity_name] = True
                data = http_resp.json()
                available = True
            else:
                data = {"status": "error", "http_status": http_resp.status_code}
                available = False

        except httpx.ConnectError:
            duration_ms = (datetime.utcnow() - started).total_seconds() * 1000
            self._entity_availability[entity_name] = False
            data = {"status": "unavailable", "reason": "connection_refused"}
            available = False
        except httpx.TimeoutException:
            duration_ms = (datetime.utcnow() - started).total_seconds() * 1000
            data = {"status": "timeout"}
            available = False
        except Exception as e:
            duration_ms = (datetime.utcnow() - started).total_seconds() * 1000
            data = {"status": "error", "error": str(e)}
            available = False

        # Record to audit store regardless of outcome
        if self._audit:
            self._audit.record_advisory(
                request_id=request_id,
                entity=entity_name,
                advisory_request=request,
                advisory_response=data,
                duration_ms=duration_ms,
                available=available,
                correlation_id=request_id,
            )

        # Record to Langfuse — advisory span under the orchestration trace
        if self._tracer and lf_trace:
            self._tracer.span_advisory(
                trace=lf_trace,
                entity=entity_name,
                request=request,
                response=data,
                duration_ms=duration_ms,
                available=available,
            )

        status = data.get("status", "ok" if available else "failed")
        log.debug(f"Advisory {entity_name}: {status} ({duration_ms:.0f}ms)", stage="ADVISE")
        return data

    # ── Decision influence annotation ──────────────────────────────────────────

    def _annotate_influence(
        self,
        request_id: str,
        advisories: AdvisorySet,
        decision: Decision,
    ):
        """
        After reasoning, annotate each advisory with whether it influenced the decision.
        This closes the audit loop — every input is traceable to the output.
        """
        if not self._audit:
            return

        trace_steps = {s.step: s for s in decision.reasoning_trace}
        gov_step = trace_steps.get("governance_gate")
        risk_step = trace_steps.get("risk_weighting")

        if advisories.sentinel:
            influenced = (
                decision.blocked
                or decision.escalate
                or (risk_step and "sentinel_risk" in risk_step.input_signals)
            )
            note = (
                f"Blocked decision: {decision.block_reason}" if decision.blocked
                else f"Risk={advisories.sentinel.risk_score:.2f} fed into reasoning"
            )
            self._audit.annotate_decision_influence(request_id, "sentinel", influenced, note)

        if advisories.cartographer:
            influenced = decision.action_type == "plan" and bool(advisories.cartographer.story_id)
            note = (
                f"Story {advisories.cartographer.story_id} informed plan decision"
                if influenced else "Story generated but decision did not require planning"
            )
            self._audit.annotate_decision_influence(request_id, "cartographer", influenced, note)

        if advisories.assembler:
            influenced = decision.action_type == "plan"
            note = (
                f"Plan {advisories.assembler.plan_id} with {advisories.assembler.estimated_days}d estimate used"
                if influenced else "Plan generated but not yet acted on"
            )
            self._audit.annotate_decision_influence(request_id, "assembler", influenced, note)

        if advisories.philosopher:
            # Philosopher influences whenever it provides insights (not only on anomaly).
            # Insights feed into advisory_synthesis step and shape decision context.
            influenced = bool(advisories.philosopher.anomaly_detected) or bool(advisories.philosopher.insights)
            if advisories.philosopher.anomaly_detected:
                note = "Anomaly detection influenced decision context"
            elif advisories.philosopher.insights:
                note = f"Insights ({len(advisories.philosopher.insights)}) informed advisory synthesis"
            else:
                note = "No insights generated — advisory noted but not influential"
            self._audit.annotate_decision_influence(request_id, "philosopher", influenced, note)

    # ── Health ─────────────────────────────────────────────────────────────────

    def get_health(self) -> Dict[str, Any]:
        health: Dict[str, Any] = {
            "events_processed": self._events_processed,
            "entity_availability": self._entity_availability,
            "http_client_ready": self._http_client is not None,
            "reasoning_engine": "active",
            "advisory_audit": "active" if self._audit else "disabled",
        }
        if self._mcp:
            health["mcp"] = self._mcp.get_metrics()
        if self._audit:
            health["audit_stats"] = self._audit.get_stats()
        return health
