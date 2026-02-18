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

# ── LLM-driven clarification ──────────────────────────────────────────────────

_CLARIFY_SYSTEM = """You are a senior technical consultant helping scope a software project.
Your job: ask ONE focused clarifying question to gather the most important missing information.

Rules:
- Ask only ONE question at a time.
- Provide 3-4 short button options.
- Read the conversation so far — don't repeat answered questions.
- If you already have enough to build a solid plan (typically after 2-4 meaningful answers),
  respond with {"done": true} to signal no more questions are needed. The system will
  automatically add a final catch-all "anything else?" prompt before planning — you do NOT
  need to add that yourself.
- Be a consultant, not a form — infer obvious answers from context (e.g. "HTML game" implies web browser).
- Never ask about things that are clearly implied by the request.
- Keep question text concise (under 12 words). Options under 6 words each.
- For questions where multiple selections make sense (e.g. "which features"), set "multi_select": true.
  The user will be able to toggle multiple options and submit them together.
- For single-choice questions (e.g. "which platform", "target age"), omit multi_select or set false.

Respond ONLY with valid JSON in one of these two forms:
{"done": true}
{"question": "...", "id": "q_<slug>", "options": ["Option A", "Option B", "Option C"], "multi_select": false}"""


_MID_QA_SYSTEM = """You are BasalMind, a technical project consultant mid-conversation.
The user has added a comment or asked a question while you were clarifying their project scope.
Respond briefly (1-3 sentences max) in a helpful, conversational tone.
If they're correcting something (e.g. timeline is too long), acknowledge it specifically and confirm you've noted it.
If they're asking a question, answer it directly.
End by gently steering them back to the current question if appropriate.
Do NOT use markdown headers. Keep it natural and concise."""


def _respond_to_mid_qa_comment(ctx: Dict[str, Any], intent: str, comment: str,
                                channel_id: str, thread_ts: Optional[str]):
    """
    Generate a brief, contextual reply to a mid-Q&A comment.
    Also records any corrective information as a context note.
    """
    import openai as _openai
    openai_key = os.getenv("OPENAI_API_KEY")

    answers = ctx.get("answers", {})
    answered_lines = "\n".join(f"- {k}: {v}" for k, v in answers.items()) or "None yet."
    initial_request = ctx.get("initial_request", "")

    user_prompt = (
        f"Original request: {initial_request}\n"
        f"Answers so far:\n{answered_lines}\n\n"
        f"User's mid-conversation comment: \"{comment}\"\n\n"
        "Respond naturally and briefly."
    )

    reply = None
    if openai_key:
        try:
            client = _openai.OpenAI(api_key=openai_key)
            messages = [
                {"role": "system", "content": _MID_QA_SYSTEM},
                {"role": "user", "content": user_prompt},
            ]
            with traced_generation(_lf, None, "conductor.mid_qa_response", "gpt-4o-mini", messages) as gen:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages,
                    max_tokens=150,
                    temperature=0.5,
                    timeout=8,
                )
                reply = response.choices[0].message.content.strip()
                gen["output"] = reply
                gen["usage"] = {
                    "input": response.usage.prompt_tokens,
                    "output": response.usage.completion_tokens,
                }
        except Exception as e:
            logger.warning(f"[MID-QA] LLM reply failed: {e}")

    if not reply:
        reply = "Got it — I've noted that. Let's continue with the current question above."

    # If comment sounds like a correction/preference, store it as a context note
    notes = ctx.get("freeform_notes", [])
    notes.append(comment)
    ctx["freeform_notes"] = notes[-10:]  # keep last 10
    save_project_context(channel_id, ctx)

    _post_to_slack(channel_id, reply, thread_ts=thread_ts)


def _llm_next_clarification(ctx: Dict[str, Any], intent: str) -> Optional[Dict[str, Any]]:
    """
    Ask the LLM what the single most valuable clarifying question is, given
    everything asked and answered so far. Returns:
      {"question": str, "id": str, "options": [str, ...]}  — ask this question
      {"done": True}                                        — enough context, proceed
      None                                                  — LLM unavailable, use fallback
    """
    import openai as _openai
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        return None

    answers = ctx.get("answers", {})
    initial_request = ctx.get("initial_request", "")
    channel_name = ctx.get("channel_name", "")

    # Build conversation context for the LLM
    answered_lines = "\n".join(f"- {k}: {v}" for k, v in answers.items()) if answers else "None yet."
    questions_asked = ctx.get("questions_asked", [])
    freeform_notes = ctx.get("freeform_notes", [])
    notes_section = ""
    if freeform_notes:
        notes_section = f"User freeform notes/corrections:\n" + "\n".join(f"- {n}" for n in freeform_notes) + "\n\n"

    user_prompt = (
        f"Project channel: #{channel_name}\n"
        f"Intent: {intent}\n"
        f"Original request: {initial_request}\n\n"
        f"Questions already asked: {questions_asked}\n"
        f"Answers received so far:\n{answered_lines}\n\n"
        f"{notes_section}"
        "What is the single most important clarifying question to ask next? "
        "Or respond with {\"done\": true} if we have enough to plan."
    )

    try:
        client = _openai.OpenAI(api_key=openai_key)
        messages = [
            {"role": "system", "content": _CLARIFY_SYSTEM},
            {"role": "user", "content": user_prompt},
        ]
        with traced_generation(_lf, None, "conductor.clarify_question", "gpt-4o-mini", messages) as gen:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                max_tokens=200,
                temperature=0.3,
                timeout=10,
            )
            raw = response.choices[0].message.content.strip()
            gen["output"] = raw
            gen["usage"] = {
                "input": response.usage.prompt_tokens,
                "output": response.usage.completion_tokens,
            }

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        parsed = json.loads(raw.strip())
        return parsed
    except Exception as e:
        logger.warning(f"[CLARIFY] LLM question generation failed: {e}")
        return None

# ── Block Kit builders ────────────────────────────────────────────────────────

def _build_clarification_blocks(question: Dict[str, Any], channel_name: str,
                                answers_so_far: Dict[str, str],
                                selected_options: Optional[List[str]] = None) -> List[Dict]:
    """
    Build Block Kit message for a single LLM-generated question.
    Supports two modes:
      - single-select: clicking an option immediately submits (default)
      - multi_select=true: buttons toggle, a "Done ✓" button finalises selections
    Always includes a 'Ready to plan →' button.
    Shows a compact answered-so-far strip if any answers exist.
    selected_options: currently toggled options (for multi-select re-renders)
    """
    q_id = question.get("id", "q_misc")
    q_text = question.get("question", "")
    options = question.get("options", [])[:4]  # Slack actions block max 5 elements; reserve 1 for "Other ✏️"
    multi = question.get("multi_select", False)
    selected_options = selected_options or []

    blocks: List[Dict] = []

    # Compact "answered so far" context strip
    if answers_so_far:
        summary = " · ".join(f"{v}" for v in answers_so_far.values())
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_Answered so far: {summary}_"}],
        })

    # Question heading — for multi-select, add a hint
    hint = " _(select all that apply, then tap Done ✓)_" if multi else ""
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{q_text}*{hint}"},
    })

    # Answer option buttons
    answer_elements = []
    for opt in options:
        slug = opt[:20].replace(' ', '_').replace('/', '_')
        if multi:
            # In multi-select: toggling an option re-renders the card with a checkmark
            is_selected = opt in selected_options
            label = f"✅ {opt}" if is_selected else opt
            answer_elements.append({
                "type": "button",
                "text": {"type": "plain_text", "text": label},
                "value": json.dumps({
                    "q_id": q_id,
                    "toggle": opt,
                    "current_selected": selected_options,
                    "multi": True,
                }),
                "action_id": f"clarify_toggle_{q_id}_{slug}",
            })
        else:
            # Single-select: clicking immediately submits the answer
            answer_elements.append({
                "type": "button",
                "text": {"type": "plain_text", "text": opt},
                "value": json.dumps({"q_id": q_id, "answer": opt}),
                "action_id": f"clarify_{q_id}_{slug}",
            })

    # Always add "Other ✏️" — lets user type a freeform reply
    answer_elements.append({
        "type": "button",
        "text": {"type": "plain_text", "text": "Other ✏️"},
        "value": json.dumps({"q_id": q_id, "answer": "__other__"}),
        "action_id": f"clarify_{q_id}__other__",
    })

    if answer_elements:
        blocks.append({
            "type": "actions",
            "block_id": f"clarify_{q_id}",
            "elements": answer_elements,
        })

    # Multi-select: show "Done ✓" submit button once at least one option is toggled
    if multi and selected_options:
        blocks.append({
            "type": "actions",
            "block_id": f"clarify_{q_id}_submit",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": f"Done ✓  ({len(selected_options)} selected)"},
                    "style": "primary",
                    "value": json.dumps({
                        "q_id": q_id,
                        "answer": ", ".join(selected_options),
                        "multi_submit": True,
                    }),
                    "action_id": f"clarify_{q_id}_submit",
                }
            ],
        })

    # Always-present "Ready to plan" button
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "actions",
        "block_id": "ready_to_plan",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "📋 Ready to plan →"},
                "style": "primary",
                "value": json.dumps({"action": "ready_to_plan"}),
                "action_id": "ready_to_plan",
            }
        ],
    })

    return blocks


def _build_answered_block(question_text: str, answer: str) -> List[Dict]:
    """
    Build the Block Kit replacement for a question card after the user answers.
    Shows just this specific Q&A pair — clean, auditable record of each exchange.
    """
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{question_text}*\n✅  {answer}",
            },
        }
    ]


def _build_approval_card(ctx: Dict[str, Any], plan: Dict[str, Any],
                          story: Dict[str, Any], risk: Dict[str, Any]) -> List[Dict]:
    """Build the Block Kit approval card shown to Jonah before execution."""
    channel_name = ctx.get("channel_name", "project")
    repo_name = ctx.get("repo_name", _suggest_repo_name(channel_name, ctx.get("initial_request", "")))
    answers = ctx.get("answers", {})
    # Show question text → answer pairs rather than raw q_id keys
    question_texts = ctx.get("question_texts", {})
    freeform_notes = ctx.get("freeform_notes", [])
    answers_lines = []
    for k, v in answers.items():
        label = question_texts.get(k, k)
        answers_lines.append(f"• *{label}:* {v}")
    if freeform_notes:
        answers_lines.append(f"• *Notes:* {'; '.join(freeform_notes)}")
    answers_text = "\n".join(answers_lines) if answers_lines else "_No preferences specified_"

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
                "text": f"*GitHub repo:* `{repo_name}`"
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "✏️ Edit name"},
                "value": json.dumps({"action": "edit_repo_name", "channel_id": ctx["channel_id"]}),
                "action_id": "edit_repo_name",
            }
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                          "text": "_Approving will create this repo, provision a Docker sandbox, and start building._"}],
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
    freeform_notes = ctx.get("freeform_notes", [])
    notes_str = ("\nUser notes: " + "; ".join(freeform_notes)) if freeform_notes else ""
    context_text = f"{text}\n\nProject: {channel_name}\nPreferences: {json.dumps(answers)}{notes_str}"
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
    repo_name = ctx.get("repo_name") or channel_name
    logger.info(f"[EXECUTE] Creating GitHub repo '{repo_name}' for project {project_id}")
    repo_result = _call_mcp("create_project_repository", {
        "project_id": project_id,
        "user_id": user_id,
        "repo_name": repo_name,
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

    # Only engage on project-worthy intents from real Slack channels.
    # Exception: if a project is already in CLARIFYING phase, allow any @mention
    # through — the user may be commenting or typing a freeform answer.
    existing_ctx = get_project_context(channel_id)
    existing_phase = existing_ctx.get("phase") if existing_ctx else None

    if channel_id == "internal":
        logger.debug(f"[DECISION] Skipping internal decision: {intent}")
        end_trace(_lf, _lf_trace, {"skipped": True, "reason": "internal channel"})
        return

    if intent not in PROJECT_INTENTS and existing_phase not in (PHASE_CLARIFYING, PHASE_AWAITING_APPROVAL):
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

    # Get or create project context (reuse the one we already fetched for the phase check)
    ctx = existing_ctx if existing_ctx is not None else None
    if ctx is None:
        ctx = create_project_context(channel_id, channel_name, user_id, text, thread_ts)
        logger.info(f"[DECISION] New project context for {channel_id}")

    # Store trace in memory (not Redis — not JSON-serializable)
    _active_traces[channel_id] = _lf_trace

    # Persist intent so clarification Q&A can reference it across interactions
    needs_save = False
    if not ctx.get("intent") and intent in PROJECT_INTENTS:
        ctx["intent"] = intent
        needs_save = True
    # Ensure thread_ts is captured
    if not ctx.get("thread_ts") and thread_ts:
        ctx["thread_ts"] = thread_ts
        needs_save = True
    if needs_save:
        save_project_context(channel_id, ctx)

    phase = ctx.get("phase", PHASE_INIT)
    logger.info(f"[DECISION] Project {channel_id} in phase {phase}")

    if phase == PHASE_INIT:
        _handle_clarification_phase(ctx, intent, text, channel_id, channel_name, thread_ts)

    elif phase == PHASE_CLARIFYING:
        # User typed a message while we're mid-Q&A. Two sub-cases:
        # 1) They clicked "Other ✏️" and are now typing their freeform answer
        # 2) They're adding commentary / asking a question

        # Strip the @mention from the text for cleaner storage/display
        clean_text = text.replace(f"<@{SLACK_BOT_USER_ID}>", "").strip()

        awaiting_q_id = ctx.get("awaiting_freeform_q_id")
        if awaiting_q_id and clean_text:
            # Special case: user is editing the repo name after clicking "✏️ Edit name"
            if awaiting_q_id == "q_repo_name":
                # Parse "repo: name" or just treat the whole clean_text as the name
                import re as _re
                repo_match = _re.search(r'repo:\s*([a-zA-Z0-9_\-]+)', clean_text, _re.IGNORECASE)
                new_repo = repo_match.group(1) if repo_match else _re.sub(r'[^a-z0-9-]', '', clean_text.lower().replace(' ', '-'))[:50]
                new_repo = new_repo.strip('-') or ctx.get("repo_name", "basalmind-project")
                ctx["repo_name"] = new_repo
                ctx.pop("awaiting_freeform_q_id", None)
                save_project_context(channel_id, ctx)
                # Re-post the approval card with updated repo name
                pending = ctx.get("pending_plan", {}) or {}
                blocks = _build_approval_card(ctx, pending.get("plan", {}), pending.get("story", {}), pending.get("risk", {}))
                approval_ts = _post_to_slack(channel_id,
                                             f"Here's the updated plan for `#{channel_name}`:",
                                             blocks=blocks, thread_ts=thread_ts)
                if approval_ts:
                    ctx["approval_msg_ts"] = approval_ts
                    save_project_context(channel_id, ctx)
                return

            # Case 1: They were prompted to type a freeform clarification answer
            logger.info(f"[DECISION] Freeform answer for {awaiting_q_id}: {clean_text[:60]!r}")
            answers = ctx.get("answers", {})
            answers[awaiting_q_id] = clean_text
            ctx["answers"] = answers
            ctx.pop("awaiting_freeform_q_id", None)

            # Update the question card to show the answered Q+A
            msg_ts = ctx.get("clarification_msg_ts")
            if msg_ts:
                question_text = ctx.get("question_texts", {}).get(awaiting_q_id, "")
                _update_slack_message(
                    channel_id, msg_ts,
                    text=f"✅ {clean_text}",
                    blocks=_build_answered_block(question_text, clean_text),
                )
            save_project_context(channel_id, ctx)

            # Continue Q&A cascade
            _handle_clarification_phase(ctx, intent, "", channel_id, channel_name, thread_ts)

        elif clean_text:
            # Case 2: Commentary or question mid-Q&A — acknowledge and continue
            logger.info(f"[DECISION] Mid-Q&A comment from user: {clean_text[:60]!r}")
            # Use LLM to generate a brief, contextual response
            _respond_to_mid_qa_comment(ctx, intent, clean_text, channel_id, thread_ts)
        else:
            logger.debug(f"[DECISION] Already in CLARIFYING phase — ignoring empty message for {channel_id}")

    elif phase == PHASE_PLANNING:
        # Already planning — this is a duplicate trigger, ignore
        logger.debug(f"[DECISION] Already planning for {channel_id}")

    elif phase == PHASE_AWAITING_APPROVAL:
        clean_text = text.replace(f"<@{SLACK_BOT_USER_ID}>", "").strip()
        awaiting_q_id = ctx.get("awaiting_freeform_q_id")

        if awaiting_q_id == "q_repo_name" and clean_text:
            # User is typing a new repo name — same handling as in PHASE_CLARIFYING
            import re as _re
            repo_match = _re.search(r'repo:\s*([a-zA-Z0-9_\-]+)', clean_text, _re.IGNORECASE)
            new_repo = repo_match.group(1) if repo_match else _re.sub(r'[^a-z0-9-]', '', clean_text.lower().replace(' ', '-'))[:50]
            new_repo = new_repo.strip('-') or ctx.get("repo_name", "basalmind-project")
            ctx["repo_name"] = new_repo
            ctx.pop("awaiting_freeform_q_id", None)
            save_project_context(channel_id, ctx)
            # Re-post the approval card with updated repo name
            pending = ctx.get("pending_plan", {}) or {}
            blocks = _build_approval_card(ctx, pending.get("plan", {}), pending.get("story", {}), pending.get("risk", {}))
            approval_ts = _post_to_slack(channel_id,
                                         f"✅ Repo name updated to `{new_repo}`. Here's the updated plan:",
                                         blocks=blocks, thread_ts=thread_ts)
            if approval_ts:
                ctx["approval_msg_ts"] = approval_ts
                save_project_context(channel_id, ctx)
        else:
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
    """
    Ask one LLM-generated clarifying question, or advance to planning if the
    LLM decides enough context has been gathered.

    The LLM sees the full conversation (request + all answers so far) and:
    - Returns the single most valuable next question with options, OR
    - Returns {"done": true} signalling it's read the room and has enough to plan
    """
    # If the catch-all was already shown and answered, go straight to planning
    if ctx.get("catch_all_asked") and "q_anything_else" in ctx.get("answers", {}):
        logger.info(f"[CLARIFY] Catch-all answered — advancing to planning")
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    # Ask LLM what to ask next (or whether to proceed)
    result = _llm_next_clarification(ctx, intent)

    if result is None:
        # LLM unavailable — proceed directly to planning
        logger.warning("[CLARIFY] LLM unavailable — proceeding to planning without clarification")
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    if result.get("done"):
        # LLM decided it has enough context.
        # Before advancing to planning, ask one final open-ended catch-all question
        # — unless we've already shown it (flag in ctx).
        if not ctx.get("catch_all_asked"):
            logger.info(f"[CLARIFY] LLM done — asking catch-all before planning")
            ctx["catch_all_asked"] = True
            q_id = "q_anything_else"
            question_texts = ctx.get("question_texts", {})
            question_texts[q_id] = "Any other requirements or constraints to note?"
            ctx["question_texts"] = question_texts
            asked = ctx.get("questions_asked", [])
            if q_id not in asked:
                asked.append(q_id)
            ctx["questions_asked"] = asked
            catch_all = {
                "question": "Any other requirements or constraints to note?",
                "id": q_id,
                "options": ["Nope, looks good!", "Keep it simple", "Make it mobile-friendly"],
                "multi_select": False,
            }
            ctx["current_question"] = catch_all
            save_project_context(channel_id, ctx)
            blocks = _build_clarification_blocks(catch_all, channel_name, ctx.get("answers", {}))
            msg_ts = _post_to_slack(channel_id, catch_all["question"], blocks=blocks, thread_ts=thread_ts)
            if msg_ts:
                ctx["clarification_msg_ts"] = msg_ts
                save_project_context(channel_id, ctx)
            return
        # Catch-all was already shown — now actually advance
        logger.info(f"[CLARIFY] Catch-all answered — advancing to planning")
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    # Track what we've asked so we don't repeat
    q_id = result.get("id", f"q_{len(ctx.get('questions_asked', []))}")
    result["id"] = q_id
    asked = ctx.get("questions_asked", [])
    if q_id not in asked:
        asked.append(q_id)
    ctx["questions_asked"] = asked
    ctx["phase"] = PHASE_CLARIFYING

    # Store current question text keyed by q_id so we can display it on the answered card
    question_texts = ctx.get("question_texts", {})
    question_texts[q_id] = result.get("question", "")
    ctx["question_texts"] = question_texts

    # Store the full current question dict for multi-select re-renders
    ctx["current_question"] = result

    save_project_context(channel_id, ctx)

    answers_so_far = ctx.get("answers", {})
    blocks = _build_clarification_blocks(result, channel_name, answers_so_far)
    msg_ts = _post_to_slack(channel_id, result.get("question", "A quick question:"),
                            blocks=blocks, thread_ts=thread_ts)
    if msg_ts:
        ctx["clarification_msg_ts"] = msg_ts
        save_project_context(channel_id, ctx)


def _suggest_repo_name(channel_name: str, initial_request: str) -> str:
    """
    Derive a clean GitHub repo name from the channel name or request.
    GitHub rules: lowercase, alphanumeric + hyphens, no leading/trailing hyphens.
    """
    import re
    # Prefer channel name if it looks descriptive (not just 'general', 'random', etc.)
    base = channel_name or ""
    if not base or base in ("general", "random", "dev", "engineering"):
        # Fall back to first 5 words of request
        words = re.sub(r'[^\w\s]', '', initial_request.lower()).split()
        base = "-".join(words[:5])
    # Sanitize: lowercase, replace spaces/underscores with hyphens, strip non-alphanum
    base = base.lower().replace(" ", "-").replace("_", "-")
    base = re.sub(r"[^a-z0-9-]", "", base)
    base = re.sub(r"-+", "-", base).strip("-")
    return base[:50] or "basalmind-project"


def _advance_to_planning(ctx: Dict[str, Any], intent: str, channel_id: str,
                          thread_ts: Optional[str]):
    """Generate plan and post approval card."""
    ctx["phase"] = PHASE_PLANNING

    # Suggest a repo name if not yet set
    if not ctx.get("repo_name"):
        ctx["repo_name"] = _suggest_repo_name(
            ctx.get("channel_name", ""), ctx.get("initial_request", "")
        )

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

    channel_name = ctx.get("channel_name", channel_id)
    # Intent hierarchy: stored in ctx > pending_plan > default
    intent = (ctx.get("intent")
              or (ctx.get("pending_plan", {}) or {}).get("intent")
              or "request.feature")

    # "Ready to plan" button — human has decided they've given enough information
    if action_id == "ready_to_plan" or action_value.get("action") == "ready_to_plan":
        logger.info(f"[INTERACTION] Ready to plan — human triggered planning for {channel_id}")
        msg_ts = ctx.get("clarification_msg_ts")
        if msg_ts:
            all_answers = ctx.get("answers", {})
            answered_lines = "\n".join(f"✅ *{k}:* {v}" for k, v in all_answers.items()) or "_No preferences specified_"
            _update_slack_message(
                channel_id, msg_ts,
                text=f"Planning #{channel_name}",
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": f"*Planning `#{channel_name}` with your inputs:*\n{answered_lines}"},
                }]
            )
        _advance_to_planning(ctx, intent, channel_id, thread_ts)
        return

    # Multi-select toggle — re-render the card with the new selection state
    if action_id.startswith("clarify_toggle_"):
        q_id = action_value.get("q_id", "")
        toggled = action_value.get("toggle", "")
        current = action_value.get("current_selected", [])
        if toggled in current:
            current.remove(toggled)
        else:
            current.append(toggled)
        # Re-render the question card with updated selections
        current_question = ctx.get("current_question", {})
        if not current_question:
            return
        msg_ts = ctx.get("clarification_msg_ts")
        if msg_ts:
            answers_so_far = ctx.get("answers", {})
            blocks = _build_clarification_blocks(current_question, channel_name, answers_so_far, current)
            _update_slack_message(channel_id, msg_ts,
                                  text=current_question.get("question", "Select options:"),
                                  blocks=blocks)
        return

    # Multi-select submit (Done ✓ button) — treat exactly like a single clarify answer
    if action_id.endswith("_submit") and action_value.get("multi_submit"):
        # Fall through to the clarify_ handler below by renaming the action_id
        action_id = "clarify_" + action_id  # triggers the startswith("clarify_") path

    # Clarification button click — record answer and ask next question
    if action_id.startswith("clarify_"):
        q_id   = action_value.get("q_id", "")
        answer = action_value.get("answer", "")
        if not q_id:
            return

        # "Other ✏️" — prompt user to type their own answer in Slack
        if answer == "__other__":
            question_text = ctx.get("question_texts", {}).get(q_id, "your question")
            msg_ts = ctx.get("clarification_msg_ts")
            if msg_ts:
                _update_slack_message(
                    channel_id, msg_ts,
                    text="Type your answer below",
                    blocks=[{
                        "type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": f"*{question_text}*\n_Type your answer as a reply below \u2014 I'll pick it up automatically._"},
                    }]
                )
            # Store a sentinel so handle_decision knows the next @mention is a freeform answer
            ctx["awaiting_freeform_q_id"] = q_id
            save_project_context(channel_id, ctx)
            return

        if answer:
            answers = ctx.get("answers", {})
            answers[q_id] = answer
            ctx["answers"] = answers
            ctx.pop("awaiting_freeform_q_id", None)
            save_project_context(channel_id, ctx)
            logger.info(f"[INTERACTION] Clarification: {q_id}={answer}")

            # Replace the answered question card with just this Q+A pair
            msg_ts = ctx.get("clarification_msg_ts")
            if msg_ts:
                question_text = ctx.get("question_texts", {}).get(q_id, "")
                _update_slack_message(
                    channel_id, msg_ts,
                    text=f"✅ {answer}",
                    blocks=_build_answered_block(question_text, answer),
                )

            # Ask next question (LLM decides whether to continue or plan)
            _handle_clarification_phase(ctx, intent, "", channel_id, channel_name, thread_ts)
        return

    # "Edit repo name" button on approval card
    if action_id == "edit_repo_name" or action_value.get("action") == "edit_repo_name":
        current_repo = ctx.get("repo_name", "")
        # Prompt user to type new name in thread
        _post_to_slack(channel_id,
                       f"Current repo name is `{current_repo}`. "
                       f"Reply with `@BasalMind repo: your-new-name` to change it.",
                       thread_ts=thread_ts)
        ctx["awaiting_freeform_q_id"] = "q_repo_name"
        question_texts = ctx.get("question_texts", {})
        question_texts["q_repo_name"] = "GitHub repo name"
        ctx["question_texts"] = question_texts
        save_project_context(channel_id, ctx)
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
