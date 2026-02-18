"""
Assembler - Technical Implementation Designer (Stage 6: LLM-enhanced)

Responsibility: Design and coordinate technical implementation.
Philosophy: Design, don't deploy. Plan, don't execute without approval.

Stage 6 upgrade: GPT-4o-mini generates real implementation plans from
the actual request context. Template fallback when LLM unavailable.

Port: 5007
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# Langfuse observability (non-fatal if unavailable)
from assembler.langfuse_client import get_langfuse, traced_generation
_lf = get_langfuse()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("assembler")

app = FastAPI(
    title="Assembler - Technical Implementation Designer",
    description="Design and coordinate technical implementation",
    version="2.0.0",
)

_consultations = 0
_plans_generated = 0
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


# ── Template fallback ─────────────────────────────────────────────────────────

PLAN_TEMPLATES = {
    "request.feature": {
        "phases": [
            {"name": "Design", "steps": ["Requirements review", "Architecture diagram", "API contract"]},
            {"name": "Implement", "steps": ["Core logic", "Tests", "Integration"]},
            {"name": "Validate", "steps": ["Code review", "QA", "Documentation"]},
        ],
        "tools_required": ["code_editor", "test_runner", "git"],
        "estimated_days": 5,
        "test_strategy": "TDD — write tests first",
        "requires_sentinel_approval": False,
    },
    "request.bug_fix": {
        "phases": [
            {"name": "Diagnose", "steps": ["Reproduce bug", "Root cause analysis", "Impact assessment"]},
            {"name": "Fix", "steps": ["Implement fix", "Add regression test"]},
            {"name": "Verify", "steps": ["Test fix", "Code review", "Deploy to staging"]},
        ],
        "tools_required": ["debugger", "test_runner", "git"],
        "estimated_days": 2,
        "test_strategy": "Regression test required before merge",
        "requires_sentinel_approval": False,
    },
    "request.deployment": {
        "phases": [
            {"name": "Pre-deploy", "steps": ["Health check", "Backup state", "Notify stakeholders"]},
            {"name": "Deploy", "steps": ["Run deployment", "Monitor metrics", "Verify services"]},
            {"name": "Post-deploy", "steps": ["Smoke tests", "Monitor 30min", "Update runbook"]},
        ],
        "tools_required": ["deployment_tool", "monitoring", "rollback_script"],
        "estimated_days": 1,
        "test_strategy": "Smoke tests + rollback plan required",
        "requires_sentinel_approval": True,
    },
    "request.analysis": {
        "phases": [
            {"name": "Gather", "steps": ["Identify data sources", "Pull relevant metrics"]},
            {"name": "Analyze", "steps": ["Statistical analysis", "Pattern identification"]},
            {"name": "Report", "steps": ["Document findings", "Provide recommendations"]},
        ],
        "tools_required": ["analytics_tool", "visualization"],
        "estimated_days": 3,
        "test_strategy": "Peer review of analysis methodology",
        "requires_sentinel_approval": False,
    },
    "default": {
        "phases": [
            {"name": "Plan", "steps": ["Define scope", "Identify resources"]},
            {"name": "Execute", "steps": ["Implement", "Test"]},
            {"name": "Close", "steps": ["Review", "Document"]},
        ],
        "tools_required": ["standard_tools"],
        "estimated_days": 3,
        "test_strategy": "Standard QA process",
        "requires_sentinel_approval": False,
    },
}


def _template_plan(intent: str, text: str) -> Dict[str, Any]:
    """Template-based fallback plan generation."""
    t = PLAN_TEMPLATES.get(intent, PLAN_TEMPLATES["default"])
    return {
        "plan_id": str(uuid.uuid4())[:8],
        "intent": intent,
        "phases": t["phases"],
        "tools_required": t["tools_required"],
        "estimated_days": t["estimated_days"],
        "test_strategy": t["test_strategy"],
        "requires_sentinel_approval": t["requires_sentinel_approval"],
        "approach": f"Standard {intent.replace('.', ' ')} workflow",
        "key_risks": [],
        "success_criteria": ["Delivered as specified", "Tests passing", "Documentation updated"],
        "source_context": text[:200],
        "generated_by": "template",
        "created_at": datetime.utcnow().isoformat(),
        "status": "draft",
    }


# ── LLM-based plan generation ─────────────────────────────────────────────────

ASSEMBLER_SYSTEM = """You are Assembler, a technical implementation designer for an AI engineering platform.

Your job is to design an implementation plan for a user request.
You design and plan — you do not deploy, execute, or trigger any side effects.
Your plan is an advisory to Basal, which makes all final decisions.

Output valid JSON with exactly these fields:
{
  "phases": [
    {"name": "Phase Name", "steps": ["step 1", "step 2", "step 3"]}
  ],
  "tools_required": ["tool1", "tool2"],
  "estimated_days": <integer>,
  "test_strategy": "one sentence describing the testing approach",
  "requires_sentinel_approval": true|false,
  "approach": "one sentence describing the implementation approach",
  "key_risks": ["risk 1", "risk 2"],
  "success_criteria": ["criterion 1", "criterion 2", "criterion 3"]
}

Guidelines:
- phases: 2-4 phases, each with 2-4 concrete steps
- estimated_days: realistic estimate (1=trivial, 5=medium feature, 10=large, 21=major)
- requires_sentinel_approval: true only for deployments, config changes, data migrations
- Return ONLY the JSON object, no preamble."""


def _llm_plan(intent: str, text: str, story_title: Optional[str], correlation_id: str) -> Optional[Dict[str, Any]]:
    """LLM-based plan generation. Returns None on failure."""
    global _llm_calls, _llm_failures
    client = _get_openai()
    if not client:
        return None

    _llm_calls += 1
    try:
        context_parts = [f"Intent: {intent}", f"Request: {text[:700] if text else '(no text)'}"]
        if story_title:
            context_parts.append(f"User story: {story_title}")

        prompt = "\n".join(context_parts) + "\n\nDesign a technical implementation plan."
        messages = [
            {"role": "system", "content": ASSEMBLER_SYSTEM},
            {"role": "user", "content": prompt},
        ]

        raw = None
        with traced_generation(_lf, None, "assembler.llm_plan", "gpt-4o-mini", messages) as gen:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                max_tokens=600,
                temperature=0.2,
                timeout=20.0,
            )
            raw = response.choices[0].message.content.strip()
            gen["output"] = raw[:500]
            gen["usage"] = {
                "input": response.usage.prompt_tokens,
                "output": response.usage.completion_tokens,
            }
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)

        required = {"phases", "tools_required", "estimated_days", "test_strategy"}
        if not required.issubset(parsed.keys()):
            raise ValueError(f"Missing fields: {required - parsed.keys()}")

        return {
            "plan_id": str(uuid.uuid4())[:8],
            "intent": intent,
            "phases": parsed["phases"],
            "tools_required": parsed["tools_required"],
            "estimated_days": int(parsed["estimated_days"]),
            "test_strategy": parsed.get("test_strategy", ""),
            "requires_sentinel_approval": bool(parsed.get("requires_sentinel_approval", False)),
            "approach": parsed.get("approach", ""),
            "key_risks": parsed.get("key_risks", [])[:5],
            "success_criteria": parsed.get("success_criteria", [])[:5],
            "source_context": text[:200],
            "generated_by": "llm",
            "created_at": datetime.utcnow().isoformat(),
            "status": "draft",
        }

    except Exception as e:
        _llm_failures += 1
        logger.warning(f"[{correlation_id}] LLM plan generation failed ({type(e).__name__}): {e}")
        return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Assembler",
        "version": "2.0.0",
        "philosophy": "Design, don't deploy. Plan, don't execute without approval.",
        "stage": "6 — LLM-enhanced",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Assembler",
        "version": "2.0.0",
        "llm_enabled": _get_openai() is not None,
        "metrics": {
            "consultations": _consultations,
            "plans_generated": _plans_generated,
            "llm_calls": _llm_calls,
            "llm_failures": _llm_failures,
        },
    }


@app.post("/consult")
async def consult(request: Request):
    """
    Technical design consultation endpoint.
    Returns implementation plan based on intent and context.
    Advisory only — no side effects.
    """
    global _consultations, _plans_generated
    _consultations += 1

    try:
        body = await request.json()
        intent = body.get("intent", "unknown")
        correlation_id = body.get("correlation_id", str(uuid.uuid4())[:8])

        context = body.get("context", {})
        text = context.get("text", "") or ""

        # Cartographer story title if available (Basal may include it in future)
        story_title = body.get("story_title")

        logger.info(f"[{correlation_id}] Technical design: intent={intent} text={text[:60]!r}")

        # Try LLM first, fall back to template
        plan = _llm_plan(intent, text, story_title, correlation_id)
        if plan is None:
            plan = _template_plan(intent, text)
            logger.debug(f"[{correlation_id}] Using template fallback")

        _plans_generated += 1
        logger.info(
            f"[{correlation_id}] Plan generated: {plan['plan_id']} "
            f"days={plan['estimated_days']} via {plan['generated_by']}"
        )

        return {
            "entity": "Assembler",
            "status": "designed",
            "correlation_id": correlation_id,
            "plan": plan,
            "implementation": {
                "approach": plan.get("approach", f"Incremental delivery for {intent.replace('.', ' ')}"),
                "key_risks": plan.get("key_risks", []),
                "test_strategy": plan.get("test_strategy", "Standard QA"),
                "success_criteria": plan.get("success_criteria", []),
            },
            # Assembler signals plan completeness — Basal decides whether to act
            "plan_complete": not plan["requires_sentinel_approval"],
            "processed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Consultation failed: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/metrics")
async def metrics():
    return {
        "service": "Assembler",
        "version": "2.0.0",
        "consultations": _consultations,
        "plans_generated": _plans_generated,
        "llm_calls": _llm_calls,
        "llm_failures": _llm_failures,
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    port = int(os.getenv("ASSEMBLER_PORT", 5007))
    uvicorn.run("assembler.main:app", host="0.0.0.0", port=port, reload=False)
