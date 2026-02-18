"""
Conductor - Human-Directed Execution Coordinator (Stage 7)

Port: 5010

Responsibility:
  - Bridge between Basal's reasoning and Jonah's Slack workspace
  - Manages the project conversation lifecycle
  - Posts Block Kit approval cards for meaningful decisions
  - Executes via MCP only after explicit human approval
  - Maintains project context (phase, answers, pending plans) in Redis

Philosophy:
  The system asks, Jonah decides, MCP executes.
  Conductor never acts autonomously on consequential work.

Lifecycle per project channel:
  INIT → CLARIFYING → PLANNING → AWAITING_APPROVAL → EXECUTING → REVIEWING
"""

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

import httpx
import nats
import redis
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

# Langfuse observability (non-fatal if unavailable)
from conductor.langfuse_client import (
    get_langfuse, start_trace, end_trace, traced_generation, traced_span
)
_lf = get_langfuse()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("conductor")

# ── Config ────────────────────────────────────────────────────────────────────

NATS_URL          = os.getenv("NATS_URL", "nats://localhost:4222")
DECISIONS_STREAM  = "BASALMIND_DECISIONS"
INTERACTIONS_STREAM = "BASALMIND_INTERACTIONS"

REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6390))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "_t9iV2e(p9voC34HpkvxirRoy%8cSYcf")

SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN")
SLACK_BOT_USER_ID = os.getenv("SLACK_BOT_USER_ID", "U09BTHZV3E0")  # @basalmind
MCP_URL          = os.getenv("MCP_SERVER_URL", "http://localhost:3100")
ASSEMBLER_URL    = os.getenv("ASSEMBLER_URL", "http://localhost:5007")
CARTOGRAPHER_URL = os.getenv("CARTOGRAPHER_URL", "http://localhost:5005")
SENTINEL_URL     = os.getenv("SENTINEL_URL", "http://localhost:5006")

CONDUCTOR_PORT   = int(os.getenv("CONDUCTOR_PORT", 5010))

# Project phases
PHASE_INIT              = "INIT"
PHASE_CLARIFYING        = "CLARIFYING"
PHASE_PLANNING          = "PLANNING"
PHASE_AWAITING_APPROVAL = "AWAITING_APPROVAL"
PHASE_EXECUTING         = "EXECUTING"
PHASE_REVIEWING         = "REVIEWING"

# Intents that trigger the full project workflow
PROJECT_INTENTS = {"request.feature", "request.bug_fix", "request.deployment"}

# ── Health state ──────────────────────────────────────────────────────────────

class _Health:
    status: str = "starting"
    decisions_received: int = 0
    approvals_processed: int = 0
    rejections_processed: int = 0
    executions_completed: int = 0
    last_decision_at: Optional[str] = None
    last_error: Optional[str] = None
    slack_connected: bool = False
    redis_connected: bool = False
    nats_connected: bool = False

_health = _Health()

# ── Redis client ──────────────────────────────────────────────────────────────

_redis: Optional[redis.Redis] = None

def _get_redis() -> Optional[redis.Redis]:
    global _redis
    if _redis is None:
        try:
            _redis = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                db=0,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            _redis.ping()
            _health.redis_connected = True
            logger.info("✅ Conductor Redis connected")
        except Exception as e:
            logger.warning(f"Redis unavailable: {e}")
            _redis = None
            _health.redis_connected = False
    return _redis

# ── Slack client ──────────────────────────────────────────────────────────────

_slack: Optional[WebClient] = None

def _get_slack() -> Optional[WebClient]:
    global _slack
    if _slack is None and SLACK_BOT_TOKEN:
        _slack = WebClient(token=SLACK_BOT_TOKEN)
        _health.slack_connected = True
        logger.info("✅ Slack client initialized")
    return _slack


# Cache channel names to avoid repeated API calls
_channel_name_cache: Dict[str, str] = {}

# In-memory store for active Langfuse traces keyed by channel_id
# Not stored in Redis — Langfuse objects are not JSON-serializable
_active_traces: Dict[str, Any] = {}

def _resolve_channel_name(channel_id: str) -> str:
    """Look up real channel name from Slack API. Falls back to channel_id."""
    if channel_id in _channel_name_cache:
        return _channel_name_cache[channel_id]
    slack = _get_slack()
    if not slack:
        return channel_id
    try:
        info = slack.conversations_info(channel=channel_id)
        name = info["channel"].get("name", channel_id)
        _channel_name_cache[channel_id] = name
        return name
    except Exception as e:
        logger.debug(f"[SLACK] Could not resolve channel name for {channel_id}: {e}")
        return channel_id


def _is_bot_mentioned(text: str) -> bool:
    """Return True if the message text contains an @mention of this bot."""
    return f"<@{SLACK_BOT_USER_ID}>" in (text or "")


# ── ProjectContext ─────────────────────────────────────────────────────────────

PROJECT_KEY_TTL = 86400 * 30  # 30 days

def _project_key(channel_id: str) -> str:
    return f"conductor:project:{channel_id}"

def get_project_context(channel_id: str) -> Optional[Dict[str, Any]]:
    r = _get_redis()
    if not r:
        return None
    try:
        raw = r.get(_project_key(channel_id))
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.warning(f"Failed to get project context: {e}")
        return None

def save_project_context(channel_id: str, ctx: Dict[str, Any]):
    r = _get_redis()
    if not r:
        return
    try:
        ctx["updated_at"] = datetime.utcnow().isoformat()
        r.setex(_project_key(channel_id), PROJECT_KEY_TTL, json.dumps(ctx))
    except Exception as e:
        logger.warning(f"Failed to save project context: {e}")

def create_project_context(channel_id: str, channel_name: str, user_id: str,
                            initial_text: str, thread_ts: Optional[str] = None) -> Dict[str, Any]:
    ctx = {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "project_id": None,
        "phase": PHASE_INIT,
        "user_id": user_id,
        "initial_request": initial_text,
        "answers": {},
        "questions_asked": [],
        "pending_plan": None,
        "artifacts": {},
        "thread_ts": thread_ts,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    save_project_context(channel_id, ctx)
    return ctx

# ── Clarification questions ───────────────────────────────────────────────────

# Questions batched by category — asked 3 at a time
CLARIFICATION_QUESTIONS = {
    "request.feature": [
        {
            "id": "platform",
            "question": "What platform should this run on?",
            "options": ["Web browser", "Mobile (iOS/Android)", "Desktop", "Both web and mobile"],
        },
        {
            "id": "tech_stack",
            "question": "Any tech stack preferences?",
            "options": ["No preference — you choose", "HTML5 / JavaScript", "React", "Python backend needed"],
        },
        {
            "id": "scope",
            "question": "What's the scope for the first version?",
            "options": ["Minimal playable prototype", "Full-featured MVP", "Just the core mechanic first"],
        },
        {
            "id": "visual_style",
            "question": "Visual style?",
            "options": ["Pixel art / retro", "Clean and minimal", "Colorful and playful", "No preference"],
        },
        {
            "id": "leaderboard",
            "question": "Should this include a leaderboard or score tracking?",
            "options": ["Yes, high score leaderboard", "Just show current score", "No scoring needed"],
        },
        {
            "id": "deployment",
            "question": "Where should this be deployed?",
            "options": ["Docker sandbox (preview only)", "Public URL I can share", "GitHub repo only for now"],
        },
    ],
    "request.bug_fix": [
        {
            "id": "reproduce",
            "question": "Can you reproduce this bug consistently?",
            "options": ["Yes, always happens", "Sometimes / intermittent", "Only happened once"],
        },
        {
            "id": "severity",
            "question": "How severe is this issue?",
            "options": ["Critical — blocking all users", "High — affecting some users", "Low — cosmetic or edge case"],
        },
        {
            "id": "environment",
            "question": "Which environment is affected?",
            "options": ["Production", "Staging only", "Local dev only", "All environments"],
        },
    ],
}

def _get_next_questions(ctx: Dict[str, Any], intent: str) -> List[Dict[str, Any]]:
    """Return the next batch of up to 3 unanswered questions."""
    all_questions = CLARIFICATION_QUESTIONS.get(intent, CLARIFICATION_QUESTIONS.get("request.feature", []))
    asked = set(ctx.get("questions_asked", []))
    answered = set(ctx.get("answers", {}).keys())
    pending = [q for q in all_questions if q["id"] not in asked and q["id"] not in answered]
    return pending[:3]

def _has_sufficient_context(ctx: Dict[str, Any]) -> bool:
    """Enough answers to generate a plan (at least platform + scope answered)."""
    answers = ctx.get("answers", {})
    return len(answers) >= 2 or len(ctx.get("questions_asked", [])) >= 3

# ── Block Kit builders ────────────────────────────────────────────────────────

def _build_clarification_blocks(questions: List[Dict[str, Any]], channel_name: str) -> List[Dict]:
    """Build Block Kit message with button-option clarification questions."""
    blocks: List[Dict] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Before I build a plan for #{channel_name}, I have a few quick questions:*"
            }
        },
        {"type": "divider"},
    ]

    for q in questions:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{q['question']}*"},
        })
        if q.get("options"):
            elements = []
            for opt in q["options"]:
                elements.append({
                    "type": "button",
                    "text": {"type": "plain_text", "text": opt},
                    "value": json.dumps({"q_id": q["id"], "answer": opt}),
                    "action_id": f"clarify_{q['id']}_{opt[:20].replace(' ', '_')}",
                })
            blocks.append({
                "type": "actions",
                "block_id": f"clarify_{q['id']}",
                "elements": elements,
            })

    return blocks


def _build_approval_card(ctx: Dict[str, Any], plan: Dict[str, Any],
                          story: Dict[str, Any], risk: Dict[str, Any]) -> List[Dict]:
    """Build the Block Kit approval card shown to Jonah before execution."""
    channel_name = ctx.get("channel_name", "project")
    answers = ctx.get("answers", {})
    answers_text = "\n".join(f"• *{k}*: {v}" for k, v in answers.items()) if answers else "_No preferences specified_"

    phases = plan.get("phases", [])
    phases_text = "\n".join(
        f"*{i+1}. {p['name']}* — {', '.join(p.get('steps', []))}"
        for i, p in enumerate(phases)
    )

    est_days = plan.get("estimated_days", "?")
    risk_score = risk.get("score", 0.0)
    risk_label = risk.get("label", "low")
    risk_emoji = "🟢" if risk_label == "low" else "🟡" if risk_label == "medium" else "🔴"

    story_title = story.get("title", "New project story")
    story_points = story.get("story_points", 3)
    acceptance = story.get("acceptance_criteria", [])
    ac_text = "\n".join(f"  ✓ {a}" for a in acceptance[:4])

    pending_plan_id = str(uuid.uuid4())[:8]

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🏗️ Build Plan: #{channel_name}"},
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Story:* {story_title}\n*Effort:* {story_points} story points · {est_days} day(s)\n{risk_emoji} *Risk:* {risk_label.title()} ({risk_score:.2f})"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Your preferences:*\n{answers_text}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Implementation phases:*\n{phases_text if phases_text else '_Plan being generated_'}"
            }
        },
    ]

    if ac_text:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Done when:*\n{ac_text}"
            }
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "_Approving will: create a GitHub repo, provision a Docker sandbox, and start building._"
            }
        },
        {
            "type": "actions",
            "block_id": "approval_actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✅ Approve & Build"},
                    "style": "primary",
                    "value": json.dumps({
                        "action": "approve",
                        "channel_id": ctx["channel_id"],
                        "plan_snapshot_id": pending_plan_id,
                    }),
                    "action_id": "approve_plan",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "Start building?"},
                        "text": {"type": "mrkdwn", "text": "This will provision a GitHub repo and Docker sandbox."},
                        "confirm": {"type": "plain_text", "text": "Yes, build it"},
                        "deny": {"type": "plain_text", "text": "Not yet"},
                    }
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "✏️ Change something"},
                    "value": json.dumps({
                        "action": "modify",
                        "channel_id": ctx["channel_id"],
                    }),
                    "action_id": "modify_plan",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "❌ Cancel"},
                    "style": "danger",
                    "value": json.dumps({
                        "action": "cancel",
                        "channel_id": ctx["channel_id"],
                    }),
                    "action_id": "cancel_plan",
                },
            ],
        },
    ]

    return blocks

# ── Slack posting ─────────────────────────────────────────────────────────────

def _post_to_slack(channel_id: str, text: str, blocks: Optional[List] = None,
                   thread_ts: Optional[str] = None) -> Optional[str]:
    """Post a message to Slack. Returns message ts."""
    slack = _get_slack()
    if not slack:
        logger.warning("Slack unavailable — cannot post message")
        return None
    try:
        kwargs: Dict[str, Any] = {
            "channel": channel_id,
            "text": text,
        }
        if blocks:
            kwargs["blocks"] = blocks
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        resp = slack.chat_postMessage(**kwargs)
        return resp["ts"]
    except SlackApiError as e:
        logger.error(f"Slack post failed: {e.response['error']}")
        return None


def _update_slack_message(channel_id: str, ts: str, text: str,
                          blocks: Optional[List] = None) -> bool:
    """Update an existing Slack message in place (chat.update)."""
    slack = _get_slack()
    if not slack or not ts:
        return False
    try:
        kwargs: Dict[str, Any] = {"channel": channel_id, "ts": ts, "text": text}
        if blocks:
            kwargs["blocks"] = blocks
        slack.chat_update(**kwargs)
        return True
    except SlackApiError as e:
        logger.warning(f"Slack update failed: {e.response['error']}")
        return False

# ── Entity consultation ───────────────────────────────────────────────────────

def _consult_entity(url: str, payload: Dict[str, Any], timeout: float = 25.0) -> Optional[Dict]:
    """Synchronous HTTP consult to an entity service."""
    try:
        resp = httpx.post(f"{url}/consult", json=payload, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Entity {url} returned {resp.status_code}")
        return None
    except Exception as e:
        logger.warning(f"Entity {url} consult failed: {e}")
        return None

def _generate_plan(ctx: Dict[str, Any], intent: str) -> Dict[str, Any]:
    """Call Assembler, Cartographer, Sentinel with full project context."""
    text = ctx.get("initial_request", "")
    answers = ctx.get("answers", {})
    channel_name = ctx.get("channel_name", "")
    user_id = ctx.get("user_id", "unknown")

    # Build enriched context for entities
    context_text = f"{text}\n\nProject: {channel_name}\nPreferences: {json.dumps(answers)}"
    correlation_id = str(uuid.uuid4())[:8]

    base_payload = {
        "intent": intent,
        "correlation_id": correlation_id,
        "context": {
            "text": context_text,
            "user_id": user_id,
            "channel": channel_name,
            "answers": answers,
        }
    }

    assembler_resp   = _consult_entity(ASSEMBLER_URL,    base_payload) or {}
    cartographer_resp = _consult_entity(CARTOGRAPHER_URL, base_payload) or {}
    sentinel_resp    = _consult_entity(SENTINEL_URL,     base_payload) or {}

    plan   = assembler_resp.get("plan", {})
    story  = cartographer_resp.get("story", {})
    risk   = sentinel_resp.get("risk_assessment", {"score": 0.2, "label": "low"})

    return {"plan": plan, "story": story, "risk": risk}

# ── MCP execution ─────────────────────────────────────────────────────────────

def _call_mcp(tool_name: str, arguments: Dict[str, Any],
              lf_trace=None) -> Dict[str, Any]:
    """Call MCP server synchronously. Optionally traces with Langfuse span."""
    with traced_span(_lf, lf_trace, f"mcp.{tool_name}",
                     {"tool": tool_name, "arguments": arguments}) as span:
        try:
            resp = httpx.post(
                f"{MCP_URL}/mcp/call-tool",
                json={"name": tool_name, "arguments": arguments},
                timeout=60.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                text = " ".join(c.get("text", "") for c in content if c.get("type") == "text")
                try:
                    result = json.loads(text)
                except Exception:
                    result = {"raw": text, "success": True}
            else:
                result = {"success": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            result = {"success": False, "error": str(e)}
        span["output"] = result
        return result

# ── Execution flow ────────────────────────────────────────────────────────────

def _execute_project(ctx: Dict[str, Any]):
    """
    Execute the approved plan:
      1. Create project in Neo4j (project_initialization)
      2. Create GitHub repo
      3. Create Docker sandbox
      4. Post progress at each step back to Slack thread
    """
    channel_id = ctx["channel_id"]
    thread_ts  = ctx.get("thread_ts")
    user_id    = ctx.get("user_id", "unknown")
    channel_name = ctx.get("channel_name", channel_id)
    lf_trace = _active_traces.pop(channel_id, None)  # retrieve trace started in handle_decision

    _post_to_slack(channel_id,
                   "⚙️ *Starting build...* I'll update this thread as each step completes.",
                   thread_ts=thread_ts)

    # Step 1 — Initialize project in Neo4j
    logger.info(f"[EXECUTE] Initializing project for {channel_id}")
    init_result = _call_mcp("project_initialization", {
        "action": "create_new",
        "project_name": channel_name,
        "user_id": user_id,
        "channel_id": channel_id,
        "initial_request": ctx.get("initial_request", ""),
    }, lf_trace=lf_trace)
    project_id = init_result.get("project_id") or str(uuid.uuid4())[:8]
    ctx["project_id"] = project_id

    if init_result.get("success", True):
        _post_to_slack(channel_id, f"✅ Project record created (ID: `{project_id}`)",
                       thread_ts=thread_ts)
    else:
        # Non-fatal — continue with generated ID
        _post_to_slack(channel_id, f"⚠️ Project record: using local ID `{project_id}` (Neo4j unavailable)",
                       thread_ts=thread_ts)

    # Step 2 — GitHub repository
    logger.info(f"[EXECUTE] Creating GitHub repo for project {project_id}")
    repo_result = _call_mcp("create_project_repository", {
        "project_id": project_id,
        "user_id": user_id,
    }, lf_trace=lf_trace)
    repo_url = repo_result.get("repo_url", "")
    if repo_url:
        ctx["artifacts"]["repo_url"] = repo_url
        ctx["artifacts"]["repo_name"] = repo_result.get("repo_name", channel_name)
        _post_to_slack(channel_id, f"✅ GitHub repo created: <{repo_url}|{ctx['artifacts']['repo_name']}>",
                       thread_ts=thread_ts)
    else:
        _post_to_slack(channel_id,
                       f"⚠️ GitHub repo: {repo_result.get('error', 'unavailable')} — continuing with sandbox",
                       thread_ts=thread_ts)

    # Step 3 — Docker sandbox
    logger.info(f"[EXECUTE] Provisioning Docker sandbox for project {project_id}")
    sandbox_result = _call_mcp("create_project_sandbox", {
        "project_id": project_id,
        "user_id": user_id,
    }, lf_trace=lf_trace)
    container_name = sandbox_result.get("container_name", "")
    if container_name:
        ctx["artifacts"]["sandbox_container"] = container_name
        ctx["artifacts"]["sandbox_workspace"] = sandbox_result.get("workspace", "")
        _post_to_slack(channel_id, f"✅ Docker sandbox ready: `{container_name}`",
                       thread_ts=thread_ts)
    else:
        _post_to_slack(channel_id,
                       f"⚠️ Sandbox: {sandbox_result.get('error', 'unavailable')} — check Docker",
                       thread_ts=thread_ts)

    # Step 4 — Generate code in sandbox (if sandbox is ready)
    container_name = ctx["artifacts"].get("sandbox_container")
    if container_name and project_id:
        _post_to_slack(channel_id, "🤖 *Generating code...* Writing the initial implementation into the sandbox.",
                       thread_ts=thread_ts)
        game_url = _generate_and_deploy(ctx, project_id, container_name, channel_id, thread_ts,
                                        lf_trace=lf_trace)
        if game_url:
            ctx["artifacts"]["game_url"] = game_url
    else:
        game_url = None

    # Update context and phase
    ctx["phase"] = PHASE_REVIEWING
    save_project_context(channel_id, ctx)

    # Final summary
    repo_link  = f"<{ctx['artifacts'].get('repo_url', '#')}|GitHub repo>" if ctx["artifacts"].get("repo_url") else "_repo pending_"
    sand_name  = ctx["artifacts"].get("sandbox_container", "_sandbox pending_")
    url_line   = f"• Game: <{game_url}|Play it here!>" if game_url else ""

    _post_to_slack(channel_id,
        f"🎉 *Project `{channel_name}` is live!*\n\n"
        f"• {repo_link}\n"
        f"• Sandbox: `{sand_name}`\n"
        f"{url_line}\n\n"
        f"_Review the game above. Tell me what to change — I'll update it and redeploy._",
        thread_ts=thread_ts)

    _health.executions_completed += 1
    logger.info(f"[EXECUTE] Project setup complete for {channel_id}")

    # Close the Langfuse trace — project has reached REVIEWING
    end_trace(_lf, lf_trace,
              {"phase": PHASE_REVIEWING, "project_id": project_id,
               "repo_url": repo_url, "game_url": game_url or ""},
              score_name="execution_success",
              score_value=1.0 if game_url else 0.5)


def _generate_and_deploy(ctx: Dict[str, Any], project_id: str, container_name: str,
                          channel_id: str, thread_ts: Optional[str],
                          lf_trace=None) -> Optional[str]:
    """
    Generate the initial implementation using OpenAI, write to sandbox, start HTTP server.
    Returns public URL on success, None on failure.
    """
    import openai as _openai

    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        logger.warning("[GENERATE] No OPENAI_API_KEY — skipping code generation")
        return None

    try:
        request_text = ctx.get("initial_request", "Build the requested project")
        answers      = ctx.get("answers", {})
        pending_plan = ctx.get("pending_plan", {})
        channel_name = ctx.get("channel_name", channel_id)

        # Build implementation prompt from gathered context
        answers_text = "\n".join(f"- {k}: {v}" for k, v in answers.items()) if answers else "No additional answers."
        plan_summary = pending_plan.get("summary", "") if isinstance(pending_plan, dict) else str(pending_plan)[:500]

        system_prompt = (
            "You are a senior software engineer generating complete, working code for a web game. "
            "Output ONLY raw code files — no markdown, no commentary, no code fences. "
            "Use plain HTML5, CSS, and vanilla JavaScript (no build step, no npm). "
            "All code must be in a single self-contained index.html file that works when opened directly in a browser. "
            "The game must be fully playable, properly styled, and include a score display."
        )

        user_prompt = (
            f"Project: {channel_name}\n"
            f"Request: {request_text}\n"
            f"Requirements:\n{answers_text}\n"
            f"Plan summary: {plan_summary}\n\n"
            "Generate a single complete index.html file implementing this game. "
            "The file should be self-contained with embedded CSS and JS. "
            "Make it look polished with a dark theme. Include: game canvas, score tracking, "
            "game over screen with restart button, and keyboard/touch controls."
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        client = _openai.OpenAI(api_key=openai_key)
        code = None
        with traced_generation(_lf, lf_trace, "conductor.code_generation",
                               "gpt-4o-mini", messages) as gen:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                max_tokens=4000,
                timeout=60,
            )
            code = response.choices[0].message.content.strip()
            gen["output"] = code[:500]  # truncate for Langfuse
            gen["usage"] = {
                "input": response.usage.prompt_tokens,
                "output": response.usage.completion_tokens,
            }

        # Strip any accidental markdown code fences
        if code.startswith("```"):
            lines = code.split("\n")
            code = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])

        logger.info(f"[GENERATE] Generated {len(code)} chars of code for project {project_id}")

        # Write to sandbox via MCP
        write_result = _call_mcp("write_file_to_sandbox", {
            "project_id": project_id,
            "file_path": "index.html",
            "content": code,
        }, lf_trace=lf_trace)
        if not write_result.get("success", False):
            logger.error(f"[GENERATE] write_file_to_sandbox failed: {write_result}")
            _post_to_slack(channel_id, f"⚠️ Code written but could not save to sandbox: {write_result.get('error', '')}", thread_ts=thread_ts)
            return None

        # Start HTTP server in sandbox
        run_result = _call_mcp("run_in_sandbox", {
            "project_id": project_id,
            "command": "pkill -f 'python3 -m http.server' 2>/dev/null || true; nohup python3 -m http.server 8080 > /tmp/server.log 2>&1 &",
            "timeout": 10,
        }, lf_trace=lf_trace)
        logger.info(f"[GENERATE] HTTP server start: {run_result}")

        # Get sandbox URL
        url_result = _call_mcp("get_sandbox_url", {"project_id": project_id},
                               lf_trace=lf_trace)
        game_url = url_result.get("url", "")
        if game_url:
            _post_to_slack(channel_id,
                           f"✅ *Code generated and deployed!*\n\nPlay it here: {game_url}",
                           thread_ts=thread_ts)
            return game_url
        else:
            _post_to_slack(channel_id, "✅ Code written to sandbox (URL not yet available — server may need a moment to start).",
                           thread_ts=thread_ts)
            return None

    except Exception as e:
        logger.error(f"[GENERATE] Code generation failed: {e}", exc_info=True)
        _post_to_slack(channel_id, f"⚠️ Code generation failed: {e}", thread_ts=thread_ts)
        return None

# ── Decision handler ──────────────────────────────────────────────────────────

def handle_decision(payload: Dict[str, Any]):
    """
    Called when Basal publishes a decision.
    Decides whether and how to engage Jonah based on project phase.
    """
    _health.decisions_received += 1
    _health.last_decision_at = datetime.utcnow().isoformat()

    channel_id  = payload.get("channel_id", "internal")
    intent      = payload.get("intent", "unknown")
    action_type = payload.get("decision", {}).get("action_type", "observe")
    event       = payload.get("event", {})
    normalized  = event.get("normalized", {}) or {}
    user_id     = normalized.get("user_id", "unknown")
    thread_ts   = normalized.get("thread_id") or normalized.get("source_timestamp")
    text        = normalized.get("text", "")
    # Resolve real channel name — normalized may have raw ID
    channel_name = normalized.get("channel_name") or _resolve_channel_name(channel_id)

    logger.info(f"[DECISION] channel={channel_id} intent={intent} action={action_type}")

    # @mention gate: only engage if message explicitly @mentions the bot
    # This prevents the bot from hijacking every human conversation in a channel
    if channel_id != "internal" and intent in PROJECT_INTENTS:
        if not _is_bot_mentioned(text):
            logger.debug(f"[DECISION] No @mention in message — ignoring ({channel_id})")
            return

    # Langfuse trace for this decision
    _lf_trace = start_trace(
        _lf,
        name="conductor.handle_decision",
        session_id=channel_id,
        user_id=user_id,
        input_data={"intent": intent, "action_type": action_type, "text": text[:500]},
        metadata={"channel_id": channel_id, "channel_name": channel_name, "source": "conductor"},
    )

    # Only engage on project-worthy intents from real Slack channels
    if channel_id == "internal" or intent not in PROJECT_INTENTS:
        logger.debug(f"[DECISION] Skipping non-project decision: {intent} / {channel_id}")
        end_trace(_lf, _lf_trace, {"skipped": True, "reason": "non-project intent"})
        return

    if action_type in ("block", "escalate"):
        # Post governance block back to channel
        _post_to_slack(channel_id,
                       f"🛑 *Governance block:* {payload.get('decision', {}).get('rationale', 'Request blocked by policy')}",
                       thread_ts=thread_ts)
        end_trace(_lf, _lf_trace, {"action": "blocked", "action_type": action_type})
        return

    # Get or create project context
    ctx = get_project_context(channel_id)
    if ctx is None:
        ctx = create_project_context(channel_id, channel_name, user_id, text, thread_ts)
        logger.info(f"[DECISION] New project context for {channel_id}")

    # Store trace in memory (not Redis — not JSON-serializable)
    _active_traces[channel_id] = _lf_trace

    # Ensure thread_ts is captured
    if not ctx.get("thread_ts") and thread_ts:
        ctx["thread_ts"] = thread_ts
        save_project_context(channel_id, ctx)

    phase = ctx.get("phase", PHASE_INIT)
    logger.info(f"[DECISION] Project {channel_id} in phase {phase}")

    if phase in (PHASE_INIT, PHASE_CLARIFYING):
        _handle_clarification_phase(ctx, intent, text, channel_id, channel_name, thread_ts)

    elif phase == PHASE_PLANNING:
        # Already planning — this is a duplicate trigger, ignore
        logger.debug(f"[DECISION] Already planning for {channel_id}")

    elif phase == PHASE_AWAITING_APPROVAL:
        # Already has an approval card out — remind
        _post_to_slack(channel_id,
                       "⏳ Waiting for your approval on the plan above — use the buttons to proceed.",
                       thread_ts=thread_ts)

    elif phase in (PHASE_EXECUTING, PHASE_REVIEWING):
        # Project active — treat as an iteration request
        logger.info(f"[DECISION] Iterative request for existing project {channel_id}")
        # Reset to clarifying for the new request
        ctx["phase"] = PHASE_CLARIFYING
        ctx["initial_request"] = text
        ctx["answers"] = {}
        ctx["questions_asked"] = []
        ctx["pending_plan"] = None
        save_project_context(channel_id, ctx)
        _handle_clarification_phase(ctx, intent, text, channel_id, channel_name, thread_ts)

    # Close trace for non-execution paths (execution paths close it in _execute_project)
    if phase not in (PHASE_AWAITING_APPROVAL,):
        trace_to_close = _active_traces.pop(channel_id, _lf_trace)
        end_trace(_lf, trace_to_close, {"phase": phase, "intent": intent, "routed": True})


def _handle_clarification_phase(ctx: Dict[str, Any], intent: str, text: str,
                                  channel_id: str, channel_name: str,
                                  thread_ts: Optional[str]):
    """Post clarification questions or advance to planning if enough context."""
    if _has_sufficient_context(ctx):
        # We have enough — move to planning
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    questions = _get_next_questions(ctx, intent)
    if not questions:
        # All questions answered
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    # Track which questions we're asking
    asked = ctx.get("questions_asked", [])
    for q in questions:
        if q["id"] not in asked:
            asked.append(q["id"])
    ctx["questions_asked"] = asked
    ctx["phase"] = PHASE_CLARIFYING
    save_project_context(channel_id, ctx)

    blocks = _build_clarification_blocks(questions, channel_name)
    msg_ts = _post_to_slack(channel_id, f"A few quick questions about `#{channel_name}`:",
                            blocks=blocks, thread_ts=thread_ts)
    if msg_ts:
        ctx["clarification_msg_ts"] = msg_ts
        save_project_context(channel_id, ctx)


def _advance_to_planning(ctx: Dict[str, Any], intent: str, channel_id: str,
                          thread_ts: Optional[str]):
    """Generate plan and post approval card."""
    ctx["phase"] = PHASE_PLANNING
    save_project_context(channel_id, ctx)

    _post_to_slack(channel_id, "✏️ *Drafting your build plan...* (this takes ~10 seconds)",
                   thread_ts=thread_ts)

    result = _generate_plan(ctx, intent)
    plan  = result["plan"]
    story = result["story"]
    risk  = result["risk"]

    # Store pending plan
    ctx["pending_plan"] = {
        "plan": plan,
        "story": story,
        "risk": risk,
        "intent": intent,
        "created_at": datetime.utcnow().isoformat(),
    }
    ctx["phase"] = PHASE_AWAITING_APPROVAL
    save_project_context(channel_id, ctx)

    blocks = _build_approval_card(ctx, plan, story, risk)
    approval_ts = _post_to_slack(channel_id,
                                  f"Here's the plan for `#{ctx.get('channel_name', channel_id)}`:",
                                  blocks=blocks, thread_ts=thread_ts)
    if approval_ts:
        ctx["approval_msg_ts"] = approval_ts
        save_project_context(channel_id, ctx)

# ── Interaction handler ───────────────────────────────────────────────────────

def handle_interaction(payload: Dict[str, Any]):
    """
    Called when Jonah clicks a button in Slack.
    Handles: clarification answers, approve/modify/cancel on approval card.
    """
    channel_id   = payload.get("channel_id", "")
    user_id      = payload.get("user_id", "")

    # Observer sends full Slack actions list — extract first action
    actions = payload.get("actions", [])
    first_action = actions[0] if actions else {}
    action_id        = first_action.get("action_id", payload.get("action_id", ""))
    action_value_raw = first_action.get("value", payload.get("action_value", "{}"))

    # message.ts is the ts of the Block Kit message the button is on
    message_obj = payload.get("message", {})
    message_ts  = message_obj.get("ts") or payload.get("message_ts")
    thread_ts   = payload.get("thread_ts") or message_ts

    logger.info(f"[INTERACTION] action={action_id} channel={channel_id} user={user_id} msg_ts={message_ts}")

    try:
        action_value = json.loads(action_value_raw) if action_value_raw else {}
    except Exception:
        action_value = {"raw": action_value_raw}

    ctx = get_project_context(channel_id)
    if ctx is None:
        logger.warning(f"[INTERACTION] No project context for {channel_id}")
        return

    # Clarification button click
    if action_id.startswith("clarify_"):
        q_id   = action_value.get("q_id", "")
        answer = action_value.get("answer", "")
        if q_id and answer:
            answers = ctx.get("answers", {})
            answers[q_id] = answer
            ctx["answers"] = answers
            save_project_context(channel_id, ctx)
            logger.info(f"[INTERACTION] Clarification: {q_id}={answer}")

            # Update the original clarification message to show selected answers
            msg_ts = ctx.get("clarification_msg_ts")
            channel_name = ctx.get("channel_name", channel_id)
            if msg_ts:
                # Rebuild blocks showing all answers so far as checkmarks
                all_answers = ctx.get("answers", {})
                answered_lines = "\n".join(
                    f"✅ *{k}:* {v}" for k, v in all_answers.items()
                )
                _update_slack_message(
                    channel_id, msg_ts,
                    text=f"Answers for #{channel_name}",
                    blocks=[
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn",
                                     "text": f"*#{channel_name} — your choices:*\n{answered_lines}"},
                        }
                    ]
                )

            # Check if we can advance
            if _has_sufficient_context(ctx):
                intent = ctx.get("pending_plan", {}).get("intent") or "request.feature"
                _post_to_slack(channel_id, "✏️ All set — drafting your build plan now...",
                               thread_ts=thread_ts)
                _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    # Approval card actions
    action = action_value.get("action", "")

    # Replace the approval card with a static confirmation so buttons disappear
    approval_msg_ts = message_ts or ctx.get("approval_msg_ts")
    channel_name = ctx.get("channel_name", channel_id)

    if action == "approve":
        _health.approvals_processed += 1
        ctx["phase"] = PHASE_EXECUTING
        save_project_context(channel_id, ctx)
        logger.info(f"[INTERACTION] APPROVED — executing for {channel_id}")
        # Replace card with "building" confirmation (removes interactive buttons)
        if approval_msg_ts:
            _update_slack_message(
                channel_id, approval_msg_ts,
                text=f"✅ Plan approved — building #{channel_name}",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": f"✅ *Plan approved* — building `#{channel_name}` now..."}
                }]
            )
        # Run execution in background thread to avoid blocking
        t = threading.Thread(target=_execute_project, args=(ctx,), daemon=True)
        t.start()

    elif action == "modify":
        ctx["phase"] = PHASE_CLARIFYING
        ctx["answers"] = {}
        ctx["questions_asked"] = []
        ctx["pending_plan"] = None
        save_project_context(channel_id, ctx)
        if approval_msg_ts:
            _update_slack_message(
                channel_id, approval_msg_ts,
                text="Plan revision requested",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "✏️ *Revision requested* — tell me what to change."}
                }]
            )
        else:
            _post_to_slack(channel_id,
                           "No problem — let's refine it. Tell me what you'd like to change:",
                           thread_ts=thread_ts)

    elif action == "cancel":
        _health.rejections_processed += 1
        ctx["phase"] = PHASE_INIT
        ctx["pending_plan"] = None
        save_project_context(channel_id, ctx)
        if approval_msg_ts:
            _update_slack_message(
                channel_id, approval_msg_ts,
                text="Plan cancelled",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "❌ *Plan cancelled.* Start fresh anytime by @mentioning me."}
                }]
            )
        else:
            _post_to_slack(channel_id,
                           "Plan cancelled. @mention me anytime to start a new project.",
                           thread_ts=thread_ts)

# ── NATS subscriber loop ──────────────────────────────────────────────────────

async def _nats_loop():
    """Subscribe to BASALMIND_DECISIONS and BASALMIND_INTERACTIONS."""
    while True:
        try:
            nc = await nats.connect(
                NATS_URL,
                name="conductor",
                max_reconnect_attempts=-1,
                reconnect_time_wait=3,
            )
            js = nc.jetstream()
            _health.nats_connected = True
            logger.info("✅ Conductor NATS connected")

            # Ensure streams exist
            for stream_name, subjects in [
                (DECISIONS_STREAM, ["decisions.>"]),
                (INTERACTIONS_STREAM, ["interactions.>"])
            ]:
                try:
                    await js.stream_info(stream_name)
                except Exception:
                    try:
                        from nats.js.api import StreamConfig, RetentionPolicy
                        await js.add_stream(StreamConfig(
                            name=stream_name,
                            subjects=subjects,
                            retention=RetentionPolicy.LIMITS,
                            max_age=86400 * 7,
                        ))
                        logger.info(f"✅ Created stream {stream_name}")
                    except Exception as e:
                        logger.warning(f"Stream {stream_name} setup: {e}")

            # Decision subscriber
            async def _on_decision(msg):
                try:
                    data = json.loads(msg.data.decode())
                    handle_decision(data)
                    await msg.ack()
                except Exception as e:
                    logger.error(f"Decision handler error: {e}", exc_info=True)
                    try:
                        await msg.nak()
                    except Exception:
                        pass

            # Interaction subscriber
            async def _on_interaction(msg):
                try:
                    data = json.loads(msg.data.decode())
                    handle_interaction(data)
                    await msg.ack()
                except Exception as e:
                    logger.error(f"Interaction handler error: {e}", exc_info=True)
                    try:
                        await msg.nak()
                    except Exception:
                        pass

            await js.subscribe("decisions.>",
                               stream=DECISIONS_STREAM,
                               durable="conductor-decisions",
                               cb=_on_decision,
                               manual_ack=True)

            try:
                await js.subscribe("interactions.>",
                                   stream=INTERACTIONS_STREAM,
                                   durable="conductor-interactions",
                                   cb=_on_interaction,
                                   manual_ack=True)
            except Exception as e:
                logger.warning(f"Interactions stream not ready yet: {e}")

            _health.status = "ok"
            logger.info("✅ Conductor subscribed to decisions.> and interactions.>")

            # Keep alive
            while not nc.is_closed:
                await asyncio.sleep(5)

        except Exception as e:
            _health.nats_connected = False
            _health.last_error = str(e)
            _health.status = "degraded"
            logger.error(f"NATS loop error (retrying in 10s): {e}")
            await asyncio.sleep(10)


def _start_nats_thread():
    """Run the async NATS loop in a background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_nats_loop())


# ── HTTP health server ────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            body = json.dumps({
                "status": _health.status,
                "service": "Conductor",
                "version": "1.0.0",
                "stage": "7 — human-directed execution",
                "nats_connected": _health.nats_connected,
                "slack_connected": _health.slack_connected,
                "redis_connected": _health.redis_connected,
                "decisions_received": _health.decisions_received,
                "approvals_processed": _health.approvals_processed,
                "rejections_processed": _health.rejections_processed,
                "executions_completed": _health.executions_completed,
                "last_decision_at": _health.last_decision_at,
                "timestamp": datetime.utcnow().isoformat(),
            }).encode()
            code = 200 if _health.status == "ok" else 503
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            body = json.dumps({
                "service": "Conductor",
                "decisions_received": _health.decisions_received,
                "approvals_processed": _health.approvals_processed,
                "rejections_processed": _health.rejections_processed,
                "executions_completed": _health.executions_completed,
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        """Direct interaction injection endpoint (for Observer to call)."""
        if self.path == "/interaction":
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw)
                handle_interaction(data)
                resp = json.dumps({"status": "ok"}).encode()
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
        pass  # Suppress default HTTP logging


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    # Initialize connections
    _get_redis()
    _get_slack()

    # Start NATS subscriber in background thread
    t = threading.Thread(target=_start_nats_thread, daemon=True)
    t.start()
    logger.info("✅ Conductor NATS thread started")

    # Start HTTP server
    server = HTTPServer(("0.0.0.0", CONDUCTOR_PORT), _Handler)
    logger.info(f"✅ Conductor HTTP server on port {CONDUCTOR_PORT} (Stage 7 — human-directed execution)")
    server.serve_forever()


if __name__ == "__main__":
    main()
