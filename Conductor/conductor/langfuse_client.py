"""
Shared Langfuse instrumentation client for BasalMind services.

Usage in any service:
    from langfuse_client import get_langfuse, traced_generation

    lf = get_langfuse()   # None if Langfuse unavailable

    # Wrap an LLM call:
    with traced_generation(lf, trace_id, name, model, prompt) as gen:
        result = openai_client.chat.completions.create(...)
        gen.end(output=result.choices[0].message.content,
                usage={"input": result.usage.prompt_tokens,
                       "output": result.usage.completion_tokens})

Design:
- Singleton Langfuse client per process (lazy init, never crashes)
- traced_generation() context manager for LLM calls
- traced_span() context manager for non-LLM spans
- All failures are non-fatal (logged as warnings)
"""

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger("langfuse_client")

_lf = None          # singleton Langfuse client
_lf_attempted = False

LANGFUSE_HOST       = os.getenv("LANGFUSE_HOST",       "http://104.236.99.69:3000")
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "pk-lf-27c16cd0-3677-4d01-821e-aff1b414403a")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "sk-lf-74bde768-3e31-4a0e-8af0-e03bba3cdf2d")


def get_langfuse():
    """Return the singleton Langfuse client, or None if unavailable."""
    global _lf, _lf_attempted
    if _lf_attempted:
        return _lf
    _lf_attempted = True
    try:
        from langfuse import Langfuse
        client = Langfuse(
            host=LANGFUSE_HOST,
            public_key=LANGFUSE_PUBLIC_KEY,
            secret_key=LANGFUSE_SECRET_KEY,
        )
        client.auth_check()
        _lf = client
        logger.info(f"✅ Langfuse client connected: {LANGFUSE_HOST}")
    except Exception as e:
        logger.warning(f"⚠️ Langfuse unavailable (non-fatal): {e}")
        _lf = None
    return _lf


def new_trace_id(lf) -> Optional[str]:
    """Generate a new Langfuse trace ID."""
    if not lf:
        return None
    try:
        return lf.create_trace_id()
    except Exception:
        return None


def start_trace(lf, name: str, session_id: str, user_id: Optional[str],
                input_data: Dict, metadata: Dict) -> Optional[Any]:
    """Start a top-level Langfuse trace. Returns trace object or None."""
    if not lf:
        return None
    try:
        from langfuse.types import TraceContext
        trace_id = lf.create_trace_id()
        ctx = TraceContext(trace_id=trace_id)
        span = lf.start_span(
            trace_context=ctx,
            name=name,
            input=input_data,
            metadata={**metadata, "session_id": session_id, "user_id": user_id},
        )
        return {"trace_id": trace_id, "ctx": ctx, "root_span": span,
                "session_id": session_id}
    except Exception as e:
        logger.warning(f"[LANGFUSE] start_trace failed: {e}")
        return None


def end_trace(lf, trace: Optional[Dict], output: Dict, score_name: Optional[str] = None,
              score_value: Optional[float] = None):
    """Close a trace, attach output and optional score."""
    if not lf or not trace:
        return
    try:
        trace["root_span"].update(output=output)
        trace["root_span"].end()
        if score_name is not None and score_value is not None:
            lf.create_score(trace_id=trace["trace_id"], name=score_name,
                            value=score_value)
        lf.flush()
    except Exception as e:
        logger.warning(f"[LANGFUSE] end_trace failed: {e}")


@contextmanager
def traced_generation(lf, trace: Optional[Dict], name: str, model: str,
                      prompt: Any) -> Generator:
    """
    Context manager that wraps an LLM call as a Langfuse generation span.

    Usage:
        with traced_generation(lf, trace, "assembler.plan", "gpt-4o-mini", messages) as gen:
            response = client.chat.completions.create(...)
            gen["output"] = response.choices[0].message.content
            gen["usage"]  = {"input": response.usage.prompt_tokens,
                             "output": response.usage.completion_tokens}
    """
    state: Dict[str, Any] = {"output": None, "usage": None, "error": None, "_span": None}

    if lf and trace:
        try:
            root_span = trace["root_span"]
            span = root_span.start_span(
                name=name,
                input={"model": model, "messages": _safe_serialize(prompt)},
                metadata={"type": "llm_generation", "model": model},
            )
            state["_span"] = span
        except Exception as e:
            logger.warning(f"[LANGFUSE] traced_generation open failed: {e}")

    try:
        yield state
    except Exception as exc:
        state["error"] = str(exc)
        raise
    finally:
        if state["_span"]:
            try:
                state["_span"].update(
                    output={"text": _safe_serialize(state.get("output")),
                            "error": state.get("error")},
                    metadata={**(state.get("usage") or {}), "model": model},
                    level="ERROR" if state.get("error") else "DEFAULT",
                )
                state["_span"].end()
            except Exception as e:
                logger.warning(f"[LANGFUSE] traced_generation close failed: {e}")


@contextmanager
def traced_span(lf, trace: Optional[Dict], name: str, input_data: Dict) -> Generator:
    """Context manager for a non-LLM span (MCP call, phase transition, etc.)."""
    state: Dict[str, Any] = {"output": None, "error": None, "_span": None}

    if lf and trace:
        try:
            state["_span"] = trace["root_span"].start_span(
                name=name,
                input=input_data,
                metadata={"type": "span"},
            )
        except Exception as e:
            logger.warning(f"[LANGFUSE] traced_span open failed: {e}")

    try:
        yield state
    except Exception as exc:
        state["error"] = str(exc)
        raise
    finally:
        if state["_span"]:
            try:
                state["_span"].update(
                    output={"result": _safe_serialize(state.get("output")),
                            "error": state.get("error")},
                    level="ERROR" if state.get("error") else "DEFAULT",
                )
                state["_span"].end()
            except Exception as e:
                logger.warning(f"[LANGFUSE] traced_span close failed: {e}")


def _safe_serialize(obj: Any, max_len: int = 2000) -> Any:
    """Truncate large objects before sending to Langfuse."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj[:max_len]
    if isinstance(obj, (dict, list)):
        import json
        try:
            s = json.dumps(obj, default=str)
            return json.loads(s[:max_len]) if len(s) > max_len else obj
        except Exception:
            return str(obj)[:max_len]
    return obj
