"""
Cartographer - Business Requirements Translator (Stage 6: LLM-enhanced)

Responsibility: Transform business needs into actionable requirements.
Philosophy: Translate, don't implement. Clarify, don't execute.

Stage 6 upgrade: GPT-4o-mini generates semantically rich user stories
from actual request text. Template fallback when LLM unavailable.

Port: 5005
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

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cartographer")

app = FastAPI(
    title="Cartographer - Business Requirements Translator",
    description="Transform business needs into actionable requirements",
    version="2.0.0",
)

# Metrics
_consultations = 0
_stories_generated = 0
_llm_calls = 0
_llm_failures = 0

# ── OpenAI client (lazy init, graceful failure) ───────────────────────────────

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


# ── Template fallback (Stage 3 baseline, always available) ────────────────────

STORY_TEMPLATES = {
    "request.feature": {
        "title_template": "As a user, I want {text} so that I can achieve my goal",
        "priority": "P2", "story_points": 5,
        "acceptance_criteria": ["Feature works as specified", "Tests pass", "Documentation updated"],
    },
    "request.bug_fix": {
        "title_template": "Fix: {text}",
        "priority": "P1", "story_points": 3,
        "acceptance_criteria": ["Bug is reproducible and fixed", "Regression tests added", "No new bugs introduced"],
    },
    "request.analysis": {
        "title_template": "Analyze: {text}",
        "priority": "P3", "story_points": 2,
        "acceptance_criteria": ["Analysis complete", "Findings documented", "Recommendations provided"],
    },
    "default": {
        "title_template": "Task: {text}",
        "priority": "P2", "story_points": 3,
        "acceptance_criteria": ["Task completed as described", "Quality verified"],
    },
}

PRIORITY_MAP = {"request.feature": "P2", "request.bug_fix": "P1", "request.analysis": "P3"}


def _template_story(intent: str, text: str, user_id: str) -> Dict[str, Any]:
    """Template-based fallback story generation."""
    t = STORY_TEMPLATES.get(intent, STORY_TEMPLATES["default"])
    snippet = text[:60] if text else "requested task"
    return {
        "story_id": str(uuid.uuid4())[:8],
        "title": t["title_template"].format(text=snippet),
        "intent": intent,
        "priority": t["priority"],
        "story_points": t["story_points"],
        "acceptance_criteria": t["acceptance_criteria"],
        "business_objective": f"Enable: {intent.replace('.', ' ')}",
        "dependencies": [],
        "risks": [],
        "requester": user_id,
        "source_text": text[:200],
        "generated_by": "template",
        "created_at": datetime.utcnow().isoformat(),
        "status": "draft",
    }


# ── LLM-based story generation ────────────────────────────────────────────────

CARTOGRAPHER_SYSTEM = """You are Cartographer, a business requirements translator for an AI engineering team.

Your sole job is to translate a user request into a structured user story.
You are an advisor — you translate and clarify, you do not plan implementation or execute anything.

Output valid JSON with exactly these fields:
{
  "title": "As a [role], I want [goal] so that [benefit]",
  "priority": "P0|P1|P2|P3",
  "story_points": <integer 1-13>,
  "acceptance_criteria": ["criterion 1", "criterion 2", "criterion 3"],
  "business_objective": "one sentence describing the business value",
  "dependencies": ["dependency 1"] or [],
  "risks": ["risk 1"] or []
}

Priority guide: P0=critical/blocking, P1=high/bug, P2=medium/feature, P3=low/analysis.
Story points: 1=trivial, 3=small, 5=medium, 8=large, 13=very large.
Return ONLY the JSON object, no preamble."""


def _llm_story(intent: str, text: str, user_id: str, correlation_id: str) -> Optional[Dict[str, Any]]:
    """LLM-based story generation. Returns None on failure (triggers template fallback)."""
    global _llm_calls, _llm_failures
    client = _get_openai()
    if not client:
        return None

    _llm_calls += 1
    try:
        prompt = f"""Intent: {intent}
User request: {text[:800] if text else "(no text provided)"}
Requester: {user_id}

Translate this into a structured user story."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": CARTOGRAPHER_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            max_tokens=500,
            temperature=0.3,
            timeout=8.0,
        )
        raw = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        parsed = json.loads(raw)

        # Validate required fields
        required = {"title", "priority", "story_points", "acceptance_criteria", "business_objective"}
        if not required.issubset(parsed.keys()):
            raise ValueError(f"Missing fields: {required - parsed.keys()}")

        story_id = str(uuid.uuid4())[:8]
        return {
            "story_id": story_id,
            "title": parsed["title"],
            "intent": intent,
            "priority": parsed.get("priority", PRIORITY_MAP.get(intent, "P2")),
            "story_points": int(parsed.get("story_points", 5)),
            "acceptance_criteria": parsed.get("acceptance_criteria", []),
            "business_objective": parsed.get("business_objective", ""),
            "dependencies": parsed.get("dependencies", []),
            "risks": parsed.get("risks", []),
            "requester": user_id,
            "source_text": text[:200],
            "generated_by": "llm",
            "created_at": datetime.utcnow().isoformat(),
            "status": "draft",
        }

    except Exception as e:
        _llm_failures += 1
        logger.warning(f"[{correlation_id}] LLM story generation failed ({type(e).__name__}): {e}")
        return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Cartographer",
        "version": "2.0.0",
        "philosophy": "Translate, don't implement. Clarify, don't execute.",
        "stage": "6 — LLM-enhanced",
    }


@app.get("/health")
async def health():
    llm_ok = _get_openai() is not None
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "Cartographer",
        "version": "2.0.0",
        "llm_enabled": llm_ok,
        "metrics": {
            "consultations": _consultations,
            "stories_generated": _stories_generated,
            "llm_calls": _llm_calls,
            "llm_failures": _llm_failures,
            "llm_success_rate": round(
                (_llm_calls - _llm_failures) / max(_llm_calls, 1) * 100, 1
            ),
        },
    }


@app.post("/consult")
async def consult(request: Request):
    """
    Primary consultation endpoint.
    Basal sends event + intent, Cartographer returns user story.
    Advisory only — no side effects.
    """
    global _consultations, _stories_generated
    _consultations += 1

    try:
        body = await request.json()
        event = body.get("event", {})
        intent = body.get("intent", "unknown")
        correlation_id = body.get("correlation_id", str(uuid.uuid4())[:8])

        context = body.get("context", {})
        text = context.get("text", "") or ""
        user_id = context.get("user_id", "unknown")

        logger.info(f"[{correlation_id}] Consultation: intent={intent} text={text[:60]!r}")

        # Try LLM first, fall back to template
        story = _llm_story(intent, text, user_id, correlation_id)
        if story is None:
            story = _template_story(intent, text, user_id)
            logger.debug(f"[{correlation_id}] Using template fallback")

        _stories_generated += 1
        logger.info(
            f"[{correlation_id}] Story generated: {story['story_id']} "
            f"priority={story['priority']} via {story['generated_by']}"
        )

        return {
            "entity": "Cartographer",
            "status": "translated",
            "correlation_id": correlation_id,
            "story": story,
            "requirements": {
                "business_objective": story.get("business_objective", ""),
                "success_criteria": story["acceptance_criteria"],
                "priority": story["priority"],
                "estimated_effort": f"{story['story_points']} story points",
            },
            "dependencies": story.get("dependencies", []),
            "risks": story.get("risks", []),
            "processed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Consultation failed: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/metrics")
async def metrics():
    return {
        "service": "Cartographer",
        "version": "2.0.0",
        "consultations": _consultations,
        "stories_generated": _stories_generated,
        "llm_calls": _llm_calls,
        "llm_failures": _llm_failures,
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    port = int(os.getenv("CARTOGRAPHER_PORT", 5005))
    uvicorn.run("cartographer.main:app", host="0.0.0.0", port=port, reload=False)
