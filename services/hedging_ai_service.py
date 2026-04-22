import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import requests


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = str(os.getenv("HEDGE_CLAUDE_MODEL", "claude-3-5-sonnet-latest") or "claude-3-5-sonnet-latest").strip()


def _truthy(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def _num(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _extract_json_block(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    # Try full-body JSON first.
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    # Fallback for fenced blocks.
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        parsed = json.loads(raw[start : end + 1])
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _build_prompt(context: Dict[str, Any]) -> str:
    return (
        "You are a hedge execution analyst. Return strictly valid JSON only.\\n"
        "Goal: choose whether to hedge immediately at market or place a pending order at more profitable levels.\\n"
        "You must remain conservative and risk-aware.\\n"
        "\\n"
        "Required JSON schema:\\n"
        "{\\n"
        "  \"decision\": \"MARKET_NOW\" | \"PENDING_ORDER\" | \"SKIP\",\\n"
        "  \"order_type\": \"BUY\" | \"SELL\",\\n"
        "  \"confidence\": 0-1 float,\\n"
        "  \"reason\": \"short text\",\\n"
        "  \"entry_price\": number|null,\\n"
        "  \"take_profit_price\": number|null,\\n"
        "  \"trailing_activation_pct\": number|null,\\n"
        "  \"notes\": \"optional\"\\n"
        "}\\n"
        "\\n"
        "Rules:\\n"
        "1) If exposure_side is LONG, hedge order_type must be SELL.\\n"
        "2) If exposure_side is SHORT, hedge order_type must be BUY.\\n"
        "3) Prefer MARKET_NOW when momentum/risk justifies immediate hedge.\\n"
        "4) Prefer PENDING_ORDER only with a realistic entry that improves expected profitability.\\n"
        "5) Never return markdown, only JSON.\\n"
        "\\n"
        f"Context JSON:\\n{json.dumps(context, ensure_ascii=True)}"
    )


def get_claude_hedge_decision(context: Dict[str, Any]) -> Dict[str, Any]:
    api_key = str(os.getenv("ANTHROPIC_API_KEY", "") or "").strip()
    if not api_key:
        return {
            "ok": False,
            "error": "missing_anthropic_api_key",
            "fallback": {
                "decision": "MARKET_NOW",
                "confidence": 0.4,
                "reason": "Anthropic key missing; using deterministic fallback.",
            },
        }

    timeout_s = max(5.0, _num("HEDGE_CLAUDE_TIMEOUT_SECONDS", 20.0))
    model = str(os.getenv("HEDGE_CLAUDE_MODEL", DEFAULT_MODEL) or DEFAULT_MODEL).strip() or DEFAULT_MODEL
    max_tokens = int(max(256, _num("HEDGE_CLAUDE_MAX_TOKENS", 700)))

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0.2,
        "messages": [
            {
                "role": "user",
                "content": _build_prompt(context),
            }
        ],
    }

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        response = requests.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=timeout_s)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"anthropic_request_failed: {exc}",
            "fallback": {
                "decision": "MARKET_NOW",
                "confidence": 0.45,
                "reason": "Anthropic request failed; using deterministic fallback.",
            },
        }

    if response.status_code >= 300:
        return {
            "ok": False,
            "error": f"anthropic_http_{response.status_code}: {response.text[:300]}",
            "fallback": {
                "decision": "MARKET_NOW",
                "confidence": 0.45,
                "reason": "Anthropic non-2xx; using deterministic fallback.",
            },
        }

    try:
        body = response.json()
    except Exception:
        return {
            "ok": False,
            "error": "anthropic_invalid_json_response",
            "fallback": {
                "decision": "MARKET_NOW",
                "confidence": 0.45,
                "reason": "Anthropic response parse failed; using deterministic fallback.",
            },
        }

    chunks = body.get("content") if isinstance(body, dict) else None
    text = ""
    if isinstance(chunks, list):
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            if str(chunk.get("type") or "") == "text":
                text += str(chunk.get("text") or "")

    parsed = _extract_json_block(text)
    if not isinstance(parsed, dict):
        return {
            "ok": False,
            "error": "anthropic_no_valid_json_decision",
            "raw": text[:500],
            "fallback": {
                "decision": "MARKET_NOW",
                "confidence": 0.45,
                "reason": "Claude output not parseable; using deterministic fallback.",
            },
        }

    parsed["decision"] = str(parsed.get("decision") or "").strip().upper()
    parsed["order_type"] = str(parsed.get("order_type") or "").strip().upper()
    parsed["received_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    return {"ok": True, "decision": parsed, "model": model}
