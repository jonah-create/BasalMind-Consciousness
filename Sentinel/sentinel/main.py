"""
Sentinel - Governance and Operational Discipline Enforcer (Stage 6: LLM-augmented)

Responsibility: Enforce governance and operational discipline.
Philosophy: Guard, don't block. Approve or reject, don't modify.

Stage 6 upgrade:
- Deterministic rules remain the authoritative governance layer (unchanged)
- LLM adds contextual risk reasoning — identifies risk factors from natural language
  that keyword matching misses (e.g. "migrate all customer data" vs "show me prod logs")
- LLM output can only INCREASE risk score, never decrease it below the rule baseline
- Sentinel never executes — it returns advisory decisions only

Port: 5006
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("sentinel")

app = FastAPI(
    title="Sentinel - Governance Enforcer",
    description="Enforce governance and operational discipline",
    version="2.0.0",
)

_consultations = 0
_approved = 0
_rejected = 0
_llm_calls = 0
_llm_failures = 0

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


# ── Deterministic governance rules (authoritative, unchanged from Stage 3) ────

BLOCKED_INTENTS: list = []  # Explicit block list — none yet

RISK_SCORING = {
    "request.deployment":    {"base_risk": 0.7, "label": "high"},
    "request.configuration": {"base_risk": 0.5, "label": "medium"},
    "request.feature":       {"base_risk": 0.3, "label": "low"},
    "request.bug_fix":       {"base_risk": 0.4, "label": "medium"},
    "system.alert":          {"base_risk": 0.8, "label": "high"},
    "default":               {"base_risk": 0.1, "label": "low"},
}

RESOURCE_LIMITS = {
    "require_approval_above_risk": 0.8,
}

HIGH_RISK_KEYWORDS = ["delete", "drop", "remove", "destroy", "production", "prod", "migrate", "wipe"]


def _deterministic_risk(intent: str, text: str) -> Dict[str, Any]:
    """Authoritative baseline risk. Fast, no network dependency."""
    config = RISK_SCORING.get(intent, RISK_SCORING["default"])
    base_risk = config["base_risk"]
    keyword_hits = sum(1 for kw in HIGH_RISK_KEYWORDS if kw in text.lower())
    score = min(base_risk + (keyword_hits * 0.1), 1.0)
    return {
        "score": round(score, 2),
        "label": config["label"],
        "factors": {
            "base_intent_risk": base_risk,
            "keyword_hits": keyword_hits,
            "keyword_adjustment": round(keyword_hits * 0.1, 2),
        },
        "require_human_approval": score >= RESOURCE_LIMITS["require_approval_above_risk"],
    }


# ── LLM contextual risk assessment ────────────────────────────────────────────

SENTINEL_SYSTEM = """You are Sentinel, a governance advisor for an AI engineering platform.

Your job is to assess the operational risk of a user request and return a JSON advisory.
You advise — you do not block, execute, or modify anything. Basal makes the final decision.

Analyze the request text for risk factors that keyword matching might miss:
- Data scope (one record vs. all records, one service vs. all services)
- Irreversibility (can this be undone? how easily?)
- Blast radius (how many users/systems are affected if this goes wrong?)
- Timing sensitivity (is this during peak hours, on-call, release window?)

Output valid JSON with exactly these fields:
{
  "contextual_risk_delta": <float between -0.1 and 0.3>,
  "risk_factors": ["factor 1", "factor 2"],
  "risk_rationale": "one sentence explaining the key risk",
  "reversible": true|false
}

contextual_risk_delta: how much to ADD to the baseline rule-based score.
  Use 0.0 if context doesn't add risk beyond the baseline.
  Use up to 0.3 for very high contextual risk (e.g., bulk data deletion, all-customer impact).
  You may never set this negative — you can only add risk, not remove it.
Return ONLY the JSON object."""


def _llm_risk_context(intent: str, text: str, baseline_score: float, correlation_id: str) -> Optional[Dict[str, Any]]:
    """LLM contextual risk analysis. Returns None on failure."""
    global _llm_calls, _llm_failures
    client = _get_openai()
    if not client:
        return None

    _llm_calls += 1
    try:
        prompt = f"""Intent: {intent}
Baseline rule-based risk score: {baseline_score:.2f}
User request: {text[:600] if text else "(no text)"}

Assess contextual risk factors not captured by the rule baseline."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SENTINEL_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            max_tokens=250,
            temperature=0.1,  # Low temperature: governance must be consistent
            timeout=6.0,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)

        # Enforce: LLM can only add risk, never subtract
        delta = float(parsed.get("contextual_risk_delta", 0.0))
        delta = max(0.0, min(delta, 0.3))  # Clamp: [0.0, 0.3]

        return {
            "contextual_risk_delta": round(delta, 2),
            "risk_factors": parsed.get("risk_factors", [])[:5],
            "risk_rationale": parsed.get("risk_rationale", "")[:200],
            "reversible": bool(parsed.get("reversible", True)),
        }

    except Exception as e:
        _llm_failures += 1
        logger.warning(f"[{correlation_id}] LLM risk assessment failed ({type(e).__name__}): {e}")
        return None


def _evaluate_policy(intent: str, risk_score: float) -> Dict[str, Any]:
    """Apply governance policies. Pure rules — no LLM."""
    if intent in BLOCKED_INTENTS:
        return {"decision": "reject", "reason": f"Intent {intent!r} is blocked by policy", "policy": "blocked_intent_list"}
    if risk_score >= RESOURCE_LIMITS["require_approval_above_risk"]:
        return {"decision": "pending_approval", "reason": f"Risk score {risk_score:.2f} requires human approval", "policy": "high_risk_threshold"}
    return {"decision": "approve", "reason": "Passes all governance checks", "policy": "standard_approval"}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Sentinel",
        "version": "2.0.0",
        "philosophy": "Guard, don't block. Approve or reject, don't modify.",
        "stage": "6 — LLM-augmented risk reasoning",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Sentinel",
        "version": "2.0.0",
        "llm_enabled": _get_openai() is not None,
        "metrics": {
            "consultations": _consultations,
            "approved": _approved,
            "rejected": _rejected,
            "llm_calls": _llm_calls,
            "llm_failures": _llm_failures,
        },
    }


@app.post("/consult")
async def consult(request: Request):
    """
    Governance consultation endpoint.
    Returns approve/reject/pending_approval decision with LLM-augmented risk assessment.
    Advisory only — Basal makes all final decisions.
    """
    global _consultations, _approved, _rejected
    _consultations += 1

    try:
        body = await request.json()
        event = body.get("event", {})
        intent = body.get("intent", "unknown")
        correlation_id = body.get("correlation_id", str(uuid.uuid4())[:8])

        context = body.get("context", {})
        text = context.get("text", "") or ""

        logger.info(f"[{correlation_id}] Governance check: intent={intent} text={text[:60]!r}")

        # Step 1: Deterministic baseline (always runs, never fails)
        baseline = _deterministic_risk(intent, text)
        baseline_score = baseline["score"]

        # Step 2: LLM contextual augmentation (additive only, gracefully degrades)
        llm_context = _llm_risk_context(intent, text, baseline_score, correlation_id)

        # Compose final risk score
        if llm_context:
            delta = llm_context["contextual_risk_delta"]
            final_score = min(round(baseline_score + delta, 2), 1.0)
            risk_label = "high" if final_score >= 0.7 else "medium" if final_score >= 0.4 else "low"
            risk_assessment = {
                "score": final_score,
                "label": risk_label,
                "factors": {
                    **baseline["factors"],
                    "llm_contextual_delta": delta,
                    "llm_risk_factors": llm_context["risk_factors"],
                    "reversible": llm_context["reversible"],
                },
                "risk_rationale": llm_context["risk_rationale"],
                "require_human_approval": final_score >= RESOURCE_LIMITS["require_approval_above_risk"],
                "assessment_method": "llm_augmented",
            }
        else:
            # Fallback: deterministic only
            risk_assessment = {
                **baseline,
                "risk_rationale": f"Rule-based: {intent} with keyword analysis",
                "assessment_method": "rule_based_only",
            }
            final_score = baseline_score

        # Step 3: Policy decision (pure rules, no LLM involvement)
        policy = _evaluate_policy(intent, final_score)

        if policy["decision"] == "approve":
            _approved += 1
        elif policy["decision"] == "reject":
            _rejected += 1

        logger.info(
            f"[{correlation_id}] Decision: {policy['decision']} "
            f"(risk={final_score:.2f}, method={risk_assessment['assessment_method']})"
        )

        return {
            "entity": "Sentinel",
            "status": "evaluated",
            "correlation_id": correlation_id,
            "decision": policy["decision"],
            "reason": policy["reason"],
            "policy": policy["policy"],
            "risk_assessment": risk_assessment,
            "governance": {
                "policies_checked": ["blocked_intent_list", "high_risk_threshold"],
                "compliance_status": "compliant" if policy["decision"] != "reject" else "non_compliant",
            },
            "processed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Consultation failed: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/metrics")
async def metrics():
    return {
        "service": "Sentinel",
        "version": "2.0.0",
        "consultations": _consultations,
        "approved": _approved,
        "rejected": _rejected,
        "llm_calls": _llm_calls,
        "llm_failures": _llm_failures,
        "approval_rate": round(_approved / max(_consultations, 1), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    port = int(os.getenv("SENTINEL_PORT", 5006))
    uvicorn.run("sentinel.main:app", host="0.0.0.0", port=port, reload=False)
