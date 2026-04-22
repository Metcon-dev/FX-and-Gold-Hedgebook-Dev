"""One-off smoke test for services.mt5_mcp_client against the local MCP server."""
from __future__ import annotations

import json
import os
import sys
import traceback

# Ensure repo root is on sys.path so services.* imports work.
REPO = os.path.dirname(os.path.abspath(__file__))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from services.mt5_mcp_client import call_mcp_tool, list_mcp_tools


def _section(title: str) -> None:
    print("\n" + "=" * 10 + f" {title} " + "=" * 10)


def main() -> int:
    _section("list_mcp_tools")
    try:
        tools = list_mcp_tools()
        print(f"tool count: {len(tools)}")
        for t in tools[:8]:
            print(f"  - {t.get('name')}: {str(t.get('description') or '')[:80]}")
    except Exception as exc:
        print(f"list_mcp_tools FAILED: {exc}")
        traceback.print_exc()
        return 1

    _section("get_account_info")
    try:
        res = call_mcp_tool("get_account_info", {})
        print(json.dumps(res, default=str, indent=2)[:2000])
    except Exception as exc:
        print(f"get_account_info FAILED: {exc}")
        traceback.print_exc()

    _section("get_symbol_price XAUUSD")
    try:
        res = call_mcp_tool("get_symbol_price", {"symbol_name": "XAUUSD"})
        print(json.dumps(res, default=str, indent=2)[:2000])
    except Exception as exc:
        print(f"get_symbol_price FAILED: {exc}")
        traceback.print_exc()

    _section("get_candles_latest XAUUSD M5 3")
    try:
        res = call_mcp_tool(
            "get_candles_latest",
            {"symbol_name": "XAUUSD", "timeframe": "M5", "count": 3},
        )
        print(json.dumps(res, default=str, indent=2)[:2000])
    except Exception as exc:
        print(f"get_candles_latest FAILED: {exc}")
        traceback.print_exc()

    _section("get_all_positions")
    try:
        res = call_mcp_tool("get_all_positions", {})
        print(json.dumps(res, default=str, indent=2)[:2000])
    except Exception as exc:
        print(f"get_all_positions FAILED: {exc}")
        traceback.print_exc()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
