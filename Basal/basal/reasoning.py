"""
Basal Reasoning Engine - Advisory Synthesis and Decision Making

This is Basal's cognitive core. It takes:
- Intent classification (what the user wants)
- Advisory inputs from entities (analyses, plans, risk scores, insights)
- Current system context

And produces:
- A decision with explicit reasoning
- A confidence-weighted action plan
- A full decision trace for observability

Basal reasons. Entities inform. MCP executes.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from basal.intent_classifier import IntentResult

logger = logging.getLogger("basal.reasoning")


# ── Advisory inputs (typed) ────────────────────────────────────────────────────

@dataclass
class SentinelAdvisory:
    """Risk and governance advisory from Sentinel."""
    decision: str           # "approve" | "reject" | "pending_approval"
    risk_score: float       # 0.0 - 1.0
    risk_label: str         # "low" | "medium" | "high"
    reason: str
    require_human_approval: bool
    available: bool = True


@dataclass
class CartographerAdvisory:
    """Requirements translation advisory from Cartographer."""
    story_id: Optional[str]
    title: Optional[str]
    priority: str           # "P0" - "P3"
    story_points: int
    acceptance_criteria: List[str]
    business_objective: Optional[str]
    available: bool = True


@dataclass
class AssemblerAdvisory:
    """Technical implementation advisory from Assembler."""
    plan_id: Optional[str]
    phases: List[Dict[str, Any]]
    tools_required: List[str]
    estimated_days: int
    requires_sentinel_approval: bool
    ready_for_execution: bool
    available: bool = True


@dataclass
class PhilosopherAdvisory:
    """Pattern insight advisory from Philosopher."""
    insights: List[str]
    anomaly_detected: bool
    trend: str
    recommendations: List[str]
    available: bool = True


@dataclass
class AdvisorySet:
    """All advisory inputs for a single decision."""
    sentinel: Optional[SentinelAdvisory] = None
    cartographer: Optional[CartographerAdvisory] = None
    assembler: Optional[AssemblerAdvisory] = None
    philosopher: Optional[PhilosopherAdvisory] = None


# ── Decision output (typed) ────────────────────────────────────────────────────

@dataclass
class ReasoningStep:
    """A single step in Basal's decision trace."""
    step: str
    input_signals: Dict[str, Any]
    conclusion: str
    confidence: float


@dataclass
class Decision:
    """
    Basal's synthesized decision. The output of the reasoning engine.

    Contains:
    - The action to take (or not take)
    - Which MCP skill to invoke (if any)
    - The complete reasoning trace
    - Confidence score
    """
    action_type: str        # observe | query | plan | execute | block | escalate
    skill: Optional[str]    # MCP skill name if action_type == "execute" or "query"
    skill_args: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    rationale: str = ""
    reasoning_trace: List[ReasoningStep] = field(default_factory=list)
    blocked: bool = False
    block_reason: Optional[str] = None
    escalate: bool = False
    escalation_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action_type": self.action_type,
            "skill": self.skill,
            "skill_args": self.skill_args,
            "confidence": round(self.confidence, 3),
            "rationale": self.rationale,
            "blocked": self.blocked,
            "block_reason": self.block_reason,
            "escalate": self.escalate,
            "escalation_reason": self.escalation_reason,
            "reasoning_trace": [
                {
                    "step": s.step,
                    "conclusion": s.conclusion,
                    "confidence": round(s.confidence, 3),
                    "signals": s.input_signals,
                }
                for s in self.reasoning_trace
            ],
            "metadata": self.metadata,
        }


# ── Advisory parser ────────────────────────────────────────────────────────────

def parse_advisories(consultations: Dict[str, Any]) -> AdvisorySet:
    """
    Parse raw entity consultation responses into typed advisory objects.
    Handles missing/unavailable entities gracefully.
    """
    advisories = AdvisorySet()

    # Sentinel
    s = consultations.get("sentinel", {})
    if s and s.get("status") not in ("unavailable", "timeout", "error"):
        risk = s.get("risk_assessment", {})
        advisories.sentinel = SentinelAdvisory(
            decision=s.get("decision", "approve"),
            risk_score=risk.get("score", 0.1),
            risk_label=risk.get("label", "low"),
            reason=s.get("reason", ""),
            require_human_approval=risk.get("require_human_approval", False),
        )
    elif s:
        advisories.sentinel = SentinelAdvisory(
            decision="approve", risk_score=0.0, risk_label="unknown",
            reason="Sentinel unavailable — defaulting to approve with low confidence",
            require_human_approval=False, available=False,
        )

    # Cartographer
    c = consultations.get("cartographer", {})
    if c and c.get("status") not in ("unavailable", "timeout", "error"):
        story = c.get("story", {})
        reqs = c.get("requirements", {})
        advisories.cartographer = CartographerAdvisory(
            story_id=story.get("story_id"),
            title=story.get("title"),
            priority=story.get("priority", "P2"),
            story_points=story.get("story_points", 3),
            acceptance_criteria=story.get("acceptance_criteria", []),
            business_objective=reqs.get("business_objective"),
        )

    # Assembler
    a = consultations.get("assembler", {})
    if a and a.get("status") not in ("unavailable", "timeout", "error"):
        plan = a.get("plan", {})
        # "plan_complete" is the v2 field name; fall back to "ready_for_execution" (v1 compat)
        plan_complete = a.get("plan_complete", a.get("ready_for_execution", False))
        advisories.assembler = AssemblerAdvisory(
            plan_id=plan.get("plan_id"),
            phases=plan.get("phases", []),
            tools_required=plan.get("tools_required", []),
            estimated_days=plan.get("estimated_days", 0),
            requires_sentinel_approval=plan.get("requires_sentinel_approval", False),
            ready_for_execution=plan_complete,
        )

    # Philosopher
    # v2: anomaly_detected is top-level; v1: nested under "patterns" — handle both
    p = consultations.get("philosopher", {})
    if p and p.get("status") not in ("unavailable", "timeout", "error"):
        patterns = p.get("patterns", {})
        anomaly = p.get("anomaly_detected", patterns.get("anomaly_detected", False))
        trend = p.get("trend", patterns.get("trend", "unknown"))
        advisories.philosopher = PhilosopherAdvisory(
            insights=p.get("insights", []),
            anomaly_detected=anomaly,
            trend=trend,
            recommendations=p.get("recommendations", []),
        )

    return advisories


# ── Reasoning engine ───────────────────────────────────────────────────────────

class ReasoningEngine:
    """
    Basal's decision-making core.

    Works through a deterministic reasoning chain:
    1. Governance gate    — can we proceed at all?
    2. Intent gate        — is this actionable?
    3. Risk weighting     — how much caution is warranted?
    4. Advisory synthesis — what do the advisors recommend?
    5. Decision formation — what should Basal do?
    6. Skill selection    — which MCP skill executes it?

    Every step is logged to the decision trace.
    """

    def reason(
        self,
        intent: IntentResult,
        advisories: AdvisorySet,
        event: Dict[str, Any],
    ) -> Decision:
        """
        Synthesize intent + advisories into a Decision.
        The entire reasoning chain is captured in Decision.reasoning_trace.
        """
        trace: List[ReasoningStep] = []
        normalized = event.get("normalized", {})
        text = (normalized.get("text", "") if isinstance(normalized, dict) else "") or ""

        # ── Step 1: Governance gate ────────────────────────────────────────────
        gov_step, blocked, block_reason = self._governance_gate(advisories, intent)
        trace.append(gov_step)

        if blocked:
            logger.info(f"[REASON] Blocked by governance: {block_reason}")
            return Decision(
                action_type="block",
                skill=None,
                confidence=0.95,
                rationale=block_reason,
                reasoning_trace=trace,
                blocked=True,
                block_reason=block_reason,
            )

        # ── Step 2: Intent gate ────────────────────────────────────────────────
        intent_step, actionable = self._intent_gate(intent)
        trace.append(intent_step)

        if not actionable:
            return Decision(
                action_type="observe",
                skill=None,
                confidence=intent_step.confidence,
                rationale=intent_step.conclusion,
                reasoning_trace=trace,
            )

        # ── Step 3: Risk weighting ─────────────────────────────────────────────
        risk_step, risk_score, needs_escalation = self._risk_weighting(advisories, intent)
        trace.append(risk_step)

        if needs_escalation:
            logger.info(f"[REASON] Escalating: risk={risk_score:.2f}")
            return Decision(
                action_type="escalate",
                skill=None,
                confidence=0.85,
                rationale=f"Risk score {risk_score:.2f} requires human decision",
                reasoning_trace=trace,
                escalate=True,
                escalation_reason=risk_step.conclusion,
            )

        # ── Step 4: Advisory synthesis ─────────────────────────────────────────
        synthesis_step, synthesis = self._synthesize_advisories(advisories, intent)
        trace.append(synthesis_step)

        # ── Step 5: Decision formation ─────────────────────────────────────────
        decision_step, action_type, rationale = self._form_decision(
            intent, advisories, synthesis, risk_score
        )
        trace.append(decision_step)

        # ── Step 6: Skill selection ────────────────────────────────────────────
        skill_step, skill, skill_args = self._select_skill(
            action_type, intent, advisories, event, text
        )
        trace.append(skill_step)

        # Aggregate confidence across trace
        overall_confidence = self._aggregate_confidence(trace)

        return Decision(
            action_type=action_type,
            skill=skill,
            skill_args=skill_args,
            confidence=overall_confidence,
            rationale=rationale,
            reasoning_trace=trace,
            metadata={
                "intent": intent.intent,
                "risk_score": risk_score,
                "entities_available": self._available_entities(advisories),
                "synthesis": synthesis,
            },
        )

    # ── Step implementations ───────────────────────────────────────────────────

    def _governance_gate(
        self, advisories: AdvisorySet, intent: IntentResult
    ) -> Tuple[ReasoningStep, bool, Optional[str]]:
        """Step 1: Check if Sentinel has blocked this action."""
        signals: Dict[str, Any] = {}
        blocked = False
        block_reason = None

        if advisories.sentinel and advisories.sentinel.available:
            signals["sentinel_decision"] = advisories.sentinel.decision
            signals["sentinel_risk"] = advisories.sentinel.risk_score
            signals["sentinel_reason"] = advisories.sentinel.reason

            if advisories.sentinel.decision == "reject":
                blocked = True
                block_reason = f"Sentinel rejected: {advisories.sentinel.reason}"

        step = ReasoningStep(
            step="governance_gate",
            input_signals=signals,
            conclusion=block_reason or "Governance check passed",
            confidence=0.95 if advisories.sentinel and advisories.sentinel.available else 0.5,
        )
        return step, blocked, block_reason

    def _intent_gate(self, intent: IntentResult) -> Tuple[ReasoningStep, bool]:
        """Step 2: Determine if intent is actionable."""
        actionable = True
        conclusion = f"Intent '{intent.intent}' is actionable"

        if intent.intent == "unknown" or intent.confidence < 0.25:
            actionable = False
            conclusion = f"Intent unclear (confidence={intent.confidence:.2f}) — observing only"
        elif intent.intent.startswith("conversation."):
            actionable = False
            conclusion = f"Conversational exchange — no action required"

        step = ReasoningStep(
            step="intent_gate",
            input_signals={
                "intent": intent.intent,
                "confidence": intent.confidence,
                "priority": intent.priority,
            },
            conclusion=conclusion,
            confidence=intent.confidence,
        )
        return step, actionable

    def _risk_weighting(
        self, advisories: AdvisorySet, intent: IntentResult
    ) -> Tuple[ReasoningStep, float, bool]:
        """Step 3: Compute risk-adjusted decision weight."""
        risk_score = 0.0
        signals: Dict[str, Any] = {}
        needs_escalation = False

        if advisories.sentinel and advisories.sentinel.available:
            risk_score = advisories.sentinel.risk_score
            signals["sentinel_risk"] = risk_score
            signals["sentinel_label"] = advisories.sentinel.risk_label
            signals["require_human_approval"] = advisories.sentinel.require_human_approval

            if advisories.sentinel.require_human_approval:
                needs_escalation = True

        # Priority can override escalation threshold
        if intent.priority == "urgent":
            signals["priority_override"] = "urgent — acting despite risk"
            needs_escalation = False  # Urgent events bypass escalation gate

        conclusion = (
            f"Escalation required: risk={risk_score:.2f}"
            if needs_escalation
            else f"Risk acceptable: {risk_score:.2f} ({signals.get('sentinel_label', 'unknown')})"
        )

        step = ReasoningStep(
            step="risk_weighting",
            input_signals=signals,
            conclusion=conclusion,
            confidence=0.9 if advisories.sentinel and advisories.sentinel.available else 0.4,
        )
        return step, risk_score, needs_escalation

    def _synthesize_advisories(
        self, advisories: AdvisorySet, intent: IntentResult
    ) -> Tuple[ReasoningStep, Dict[str, Any]]:
        """Step 4: Synthesize all advisory inputs into a coherent picture."""
        synthesis: Dict[str, Any] = {
            "has_story": False,
            "has_plan": False,
            "has_insights": False,
            "ready_for_execution": False,
            "estimated_effort": None,
            "key_insights": [],
        }
        signals: Dict[str, Any] = {}

        if advisories.cartographer:
            synthesis["has_story"] = True
            synthesis["story_priority"] = advisories.cartographer.priority
            synthesis["estimated_effort"] = f"{advisories.cartographer.story_points} points"
            signals["story_title"] = advisories.cartographer.title
            signals["story_priority"] = advisories.cartographer.priority

        if advisories.assembler:
            synthesis["has_plan"] = True
            synthesis["plan_phases"] = len(advisories.assembler.phases)
            synthesis["estimated_days"] = advisories.assembler.estimated_days
            synthesis["ready_for_execution"] = advisories.assembler.ready_for_execution
            signals["plan_phases"] = len(advisories.assembler.phases)
            signals["estimated_days"] = advisories.assembler.estimated_days

        if advisories.philosopher:
            synthesis["has_insights"] = True
            synthesis["key_insights"] = advisories.philosopher.insights[:2]
            synthesis["anomaly"] = advisories.philosopher.anomaly_detected
            signals["insights"] = advisories.philosopher.insights[:2]
            signals["anomaly_detected"] = advisories.philosopher.anomaly_detected

        available = [k for k in ["cartographer", "assembler", "philosopher", "sentinel"]
                     if getattr(advisories, k) is not None]

        step = ReasoningStep(
            step="advisory_synthesis",
            input_signals=signals,
            conclusion=(
                f"Synthesized {len(available)} advisories — "
                f"{'ready for execution' if synthesis['ready_for_execution'] else 'plan draft ready'}"
            ),
            confidence=min(0.9, 0.4 + len(available) * 0.15),
        )
        return step, synthesis

    def _form_decision(
        self,
        intent: IntentResult,
        advisories: AdvisorySet,
        synthesis: Dict[str, Any],
        risk_score: float,
    ) -> Tuple[ReasoningStep, str, str]:
        """Step 5: Form the action decision from all available context."""
        signals = {
            "intent": intent.intent,
            "priority": intent.priority,
            "risk_score": risk_score,
            "synthesis": synthesis,
        }

        # Decision logic — ordered by priority
        if intent.priority == "urgent":
            action_type = "execute"
            rationale = f"Urgent {intent.intent} — immediate action required"

        elif intent.intent == "query.status":
            action_type = "query"
            rationale = "Status query — retrieving system state via MCP"

        elif intent.intent == "query.information":
            action_type = "query"
            rationale = "Information request — querying available data sources"

        elif intent.intent in ("request.feature", "request.bug_fix", "request.analysis"):
            if synthesis.get("has_story") and synthesis.get("has_plan"):
                if synthesis.get("ready_for_execution"):
                    # Stage 6: promote to action_type="execute" once Sentinel-approved
                    # MCP task-creation skill is wired. Currently "plan" = dry-run logged only.
                    action_type = "plan"
                    rationale = (
                        f"Story and plan ready: {synthesis.get('estimated_effort', '?')} / "
                        f"{synthesis.get('estimated_days', '?')} days — planning logged"
                    )
                else:
                    action_type = "plan"
                    rationale = f"Requirements captured, plan drafted — awaiting approval"
            else:
                action_type = "plan"
                rationale = f"Capturing {intent.intent} — advisory context building"

        elif intent.intent == "request.deployment":
            action_type = "plan"
            rationale = "Deployment request captured — Sentinel must approve before execution"

        elif intent.intent == "request.configuration":
            action_type = "plan"
            rationale = "Configuration change noted — plan drafted for review"

        elif intent.intent == "system.alert":
            action_type = "execute"
            rationale = "System alert — querying health immediately"

        else:
            action_type = "observe"
            rationale = f"Observing {intent.intent} — no immediate action indicated"

        step = ReasoningStep(
            step="decision_formation",
            input_signals=signals,
            conclusion=f"Decision: {action_type} — {rationale}",
            confidence=0.8,
        )
        return step, action_type, rationale

    def _select_skill(
        self,
        action_type: str,
        intent: IntentResult,
        advisories: AdvisorySet,
        event: Dict[str, Any],
        text: str,
    ) -> Tuple[ReasoningStep, Optional[str], Dict[str, Any]]:
        """Step 6: Select MCP skill and build its arguments from advisory context."""
        skill: Optional[str] = None
        args: Dict[str, Any] = {}
        signals: Dict[str, Any] = {"action_type": action_type}

        if action_type in ("execute", "query"):
            if intent.intent == "query.status" or intent.intent == "system.alert":
                skill = "check_service_health"
                args = {"app_name": "BasalMindAPI", "include_metrics": True}

            elif intent.intent == "query.information":
                skill = "execute_database_query"
                args = {
                    "database": "neo4j",
                    "query": "MATCH (n) RETURN labels(n) AS type, count(n) AS count ORDER BY count DESC LIMIT 5",
                    "read_only": True,
                }

            elif intent.priority == "urgent":
                skill = "check_service_health"
                args = {"app_name": "BasalMindAPI", "include_metrics": True}

            signals["skill_selected"] = skill
            signals["rationale"] = f"MCP skill for {action_type}/{intent.intent}"

        elif action_type == "plan":
            # For plan actions: record via task management if we have advisory data
            if advisories.cartographer and advisories.cartographer.story_id:
                # Could invoke manage_tasks in future - currently log only
                signals["story_id"] = advisories.cartographer.story_id
                signals["plan_id"] = advisories.assembler.plan_id if advisories.assembler else None
                signals["note"] = "Plan logged; MCP task creation in Stage 6+"
            skill = None  # No side effect until Sentinel-approved execution

        conclusion = (
            f"Skill selected: {skill}" if skill
            else f"No MCP invocation for action_type={action_type}"
        )

        step = ReasoningStep(
            step="skill_selection",
            input_signals=signals,
            conclusion=conclusion,
            confidence=0.9 if skill else 0.7,
        )
        return step, skill, args

    # ── Utilities ──────────────────────────────────────────────────────────────

    def _aggregate_confidence(self, trace: List[ReasoningStep]) -> float:
        """Aggregate confidence scores across reasoning trace."""
        if not trace:
            return 0.0
        # Weighted average — later steps carry more weight
        weights = [0.5 ** (len(trace) - i - 1) for i in range(len(trace))]
        total_w = sum(weights)
        return round(sum(s.confidence * w for s, w in zip(trace, weights)) / total_w, 3)

    def _available_entities(self, advisories: AdvisorySet) -> List[str]:
        return [
            name for name, advisory in [
                ("sentinel", advisories.sentinel),
                ("cartographer", advisories.cartographer),
                ("assembler", advisories.assembler),
                ("philosopher", advisories.philosopher),
            ]
            if advisory is not None and getattr(advisory, "available", True)
        ]
