"""Synchronous wrapper around the local MetaTrader MCP server (SSE transport).

The MCP Python library is async-only; this module exposes a small sync API
that opens a fresh SSE session per call. That keeps the orchestrator code
simple and avoids stale-session bugs across the listener thread's idle
gaps. Per-call setup is sub-second on localhost, which is fine for hedge
decisioning.
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional

from mcp import ClientSession
from mcp.client.sse import sse_client


def _mcp_url() -> str:
    raw = str(os.getenv("MT5_MCP_URL", "") or "").strip()
    if raw:
        return raw
    host = str(os.getenv("MT5_MCP_HOST", "127.0.0.1") or "127.0.0.1").strip() or "127.0.0.1"
    port = str(os.getenv("MT5_MCP_PORT", "8080") or "8080").strip() or "8080"
    return f"http://{host}:{port}/sse"


def _timeout() -> float:
    raw = str(os.getenv("MT5_MCP_TIMEOUT_SECONDS", "20") or "20").strip()
    try:
        return max(2.0, float(raw))
    except Exception:
        return 20.0


def _coerce_jsonable(value: Any) -> Any:
    """MCP tool results may be content blocks, dicts, models, or strings.
    Convert to a JSON-friendly structure for the Anthropic tool_result payload.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_coerce_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _coerce_jsonable(v) for k, v in value.items()}
    if hasattr(value, "model_dump"):
        try:
            return _coerce_jsonable(value.model_dump())
        except Exception:
            pass
    if hasattr(value, "dict"):
        try:
            return _coerce_jsonable(value.dict())
        except Exception:
            pass
    return str(value)


def _flatten_call_result(result: Any) -> Dict[str, Any]:
    """Normalize an MCP CallToolResult into {ok, text, structured, is_error}."""
    is_error = bool(getattr(result, "isError", False))
    structured = getattr(result, "structuredContent", None)
    structured_clean = _coerce_jsonable(structured) if structured is not None else None

    text_parts: List[str] = []
    content = getattr(result, "content", None) or []
    for block in content:
        block_type = getattr(block, "type", None) or (block.get("type") if isinstance(block, dict) else None)
        if block_type == "text":
            text = getattr(block, "text", None)
            if text is None and isinstance(block, dict):
                text = block.get("text")
            if text:
                text_parts.append(str(text))
    text = "\n".join(text_parts).strip()

    return {
        "ok": not is_error,
        "is_error": is_error,
        "text": text,
        "structured": structured_clean,
    }


async def _async_list_tools(url: str, timeout_s: float) -> List[Dict[str, Any]]:
    async with sse_client(url, timeout=timeout_s, sse_read_timeout=timeout_s * 4) as (read, write):
        async with ClientSession(read, write) as session:
            await asyncio.wait_for(session.initialize(), timeout=timeout_s)
            tools_resp = await asyncio.wait_for(session.list_tools(), timeout=timeout_s)
            out: List[Dict[str, Any]] = []
            for t in tools_resp.tools or []:
                schema = getattr(t, "inputSchema", None) or {"type": "object", "properties": {}}
                out.append(
                    {
                        "name": str(getattr(t, "name", "") or ""),
                        "description": str(getattr(t, "description", "") or ""),
                        "input_schema": _coerce_jsonable(schema),
                    }
                )
            return out


async def _async_call_tool(url: str, name: str, args: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    async with sse_client(url, timeout=timeout_s, sse_read_timeout=timeout_s * 4) as (read, write):
        async with ClientSession(read, write) as session:
            await asyncio.wait_for(session.initialize(), timeout=timeout_s)
            result = await asyncio.wait_for(session.call_tool(name, args or {}), timeout=timeout_s)
            return _flatten_call_result(result)


def _run(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Caller is already inside an event loop; run in a fresh loop on a thread.
            import threading

            box: Dict[str, Any] = {}

            def _runner() -> None:
                box["value"] = asyncio.run(coro)

            t = threading.Thread(target=_runner, daemon=True)
            t.start()
            t.join()
            if "value" in box:
                return box["value"]
            raise RuntimeError("mt5_mcp_client: thread runner produced no value")
    except RuntimeError:
        pass
    return asyncio.run(coro)


def list_mcp_tools() -> List[Dict[str, Any]]:
    """Return the MT5 MCP server tool catalog as Anthropic-compatible dicts."""
    return _run(_async_list_tools(_mcp_url(), _timeout()))


def call_mcp_tool(name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Call an MT5 MCP tool by name. Returns {ok, is_error, text, structured}."""
    return _run(_async_call_tool(_mcp_url(), str(name), dict(arguments or {}), _timeout()))


def to_anthropic_tools(mcp_tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Translate MCP tool descriptors to Anthropic tool definitions."""
    out: List[Dict[str, Any]] = []
    for t in mcp_tools or []:
        name = str(t.get("name") or "").strip()
        if not name:
            continue
        schema = t.get("input_schema") or {"type": "object", "properties": {}}
        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}
        out.append(
            {
                "name": name,
                "description": str(t.get("description") or ""),
                "input_schema": schema,
            }
        )
    return out


def render_tool_result_for_anthropic(result: Dict[str, Any]) -> str:
    """Render an MCP tool result into a single string content block for Anthropic.
    Prefer structured content when present (more compact), fall back to text.
    """
    if not isinstance(result, dict):
        return json.dumps({"ok": False, "error": "tool_result_invalid"})
    structured = result.get("structured")
    if structured is not None:
        try:
            return json.dumps({"ok": bool(result.get("ok")), "structured": structured}, default=str)[:60000]
        except Exception:
            pass
    text = str(result.get("text") or "")
    if not text and result.get("is_error"):
        text = "(tool returned error with no message)"
    return json.dumps({"ok": bool(result.get("ok")), "text": text}, default=str)[:60000]
