import json
import os
import sqlite3
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from services.hedging_ai_service import get_claude_hedge_decision
from services.mt5_execution_service import (
    estimate_lot_size_from_exposure,
    estimate_lot_size_from_ounces,
    get_symbol_contract_size,
    get_position_by_ticket,
    get_symbol_tick,
    mt5_connect,
    mt5_shutdown,
    place_market_order,
    place_pending_order,
    get_position_close_summary,
    update_position_sl,
)
from services.trademc_service import fetch_trademc_trades
from services.trademc_service import get_company_name


def _truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _num(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _db_path() -> str:
    return str(os.getenv("LEDGER_DB_PATH", "") or "fx_trading_ledger.db").strip() or "fx_trading_ledger.db"


def _connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_hedging_tables() -> None:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hedge_service_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS hedge_service_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trademc_trade_id INTEGER,
            trade_ref TEXT,
            event_type TEXT,
            event_status TEXT,
            exposure_side TEXT,
            exposure_value REAL,
            symbol TEXT,
            hedge_order_type TEXT,
            execution_mode TEXT,
            mt5_order_ticket INTEGER,
            mt5_position_ticket INTEGER,
            entry_price REAL,
            stop_loss REAL,
            take_profit REAL,
            trailing_enabled INTEGER DEFAULT 1,
            trailing_distance REAL,
            ai_payload_json TEXT,
            error_text TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hedge_events_trade_id ON hedge_service_events(trademc_trade_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hedge_events_status ON hedge_service_events(event_status)")
    conn.commit()
    conn.close()


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _state_get(key: str) -> Optional[str]:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM hedge_service_state WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return str(row["value"] or "")


def _state_set(key: str, value: str) -> None:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hedge_service_state (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, str(value), _now_iso()),
    )
    conn.commit()
    conn.close()


def _event_exists_for_trade(trade_id: int) -> bool:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM hedge_service_events WHERE trademc_trade_id = ? LIMIT 1", (int(trade_id),))
    row = cur.fetchone()
    conn.close()
    return row is not None


def _insert_event(event: Dict[str, Any]) -> int:
    now = _now_iso()
    payload = dict(event)
    payload.setdefault("created_at", now)
    payload.setdefault("updated_at", now)
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hedge_service_events (
            trademc_trade_id, trade_ref, event_type, event_status,
            exposure_side, exposure_value, symbol, hedge_order_type,
            execution_mode, mt5_order_ticket, mt5_position_ticket,
            entry_price, stop_loss, take_profit, trailing_enabled,
            trailing_distance, ai_payload_json, error_text, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("trademc_trade_id"),
            payload.get("trade_ref"),
            payload.get("event_type"),
            payload.get("event_status"),
            payload.get("exposure_side"),
            payload.get("exposure_value"),
            payload.get("symbol"),
            payload.get("hedge_order_type"),
            payload.get("execution_mode"),
            payload.get("mt5_order_ticket"),
            payload.get("mt5_position_ticket"),
            payload.get("entry_price"),
            payload.get("stop_loss"),
            payload.get("take_profit"),
            1 if payload.get("trailing_enabled", True) else 0,
            payload.get("trailing_distance"),
            payload.get("ai_payload_json"),
            payload.get("error_text"),
            payload.get("created_at"),
            payload.get("updated_at"),
        ),
    )
    new_id = int(cur.lastrowid or 0)
    conn.commit()
    conn.close()
    return new_id


def _update_event(event_id: int, patch: Dict[str, Any]) -> None:
    if event_id <= 0:
        return
    keys = []
    vals: List[Any] = []
    for k, v in patch.items():
        keys.append(f"{k} = ?")
        vals.append(v)
    keys.append("updated_at = ?")
    vals.append(_now_iso())
    vals.append(int(event_id))
    sql = f"UPDATE hedge_service_events SET {', '.join(keys)} WHERE id = ?"
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(sql, vals)
    conn.commit()
    conn.close()


def _fetch_open_events() -> List[sqlite3.Row]:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM hedge_service_events
        WHERE event_status IN ('placed', 'pending')
          AND trailing_enabled = 1
          AND mt5_position_ticket IS NOT NULL
        ORDER BY id DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _has_usdzar_offset_event(trade_id: int) -> bool:
    conn = _connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM hedge_service_events
        WHERE trademc_trade_id = ?
          AND event_type = 'usdzar_offset'
        LIMIT 1
        """,
        (int(trade_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def _derive_exposure_side_and_value(trade: Dict[str, Any]) -> Dict[str, Any]:
    weight = float(trade.get("weight") or 0.0)
    side = "LONG" if weight >= 0 else "SHORT"
    exposure_abs = abs(weight)
    # Convert grams to approximate notional USD for downstream sizing.
    usd_oz = float(trade.get("usd_per_troy_ounce_confirmed") or 0.0)
    if usd_oz <= 0:
        zar_oz = float(trade.get("zar_per_troy_ounce_confirmed") or trade.get("zar_per_troy_ounce") or 0.0)
        zar_to_usd = float(trade.get("zar_to_usd_confirmed") or trade.get("zar_to_usd") or 0.0)
        if zar_oz > 0 and zar_to_usd > 0:
            usd_oz = zar_oz / zar_to_usd
    usd_notional = (exposure_abs / 31.1035) * usd_oz if usd_oz > 0 else exposure_abs
    signed = usd_notional if side == "LONG" else -usd_notional
    return {
        "side": side,
        "signed_exposure": signed,
        "abs_exposure": abs(signed),
    }


def _default_side_for_exposure(exposure_side: str) -> str:
    return "SELL" if str(exposure_side).upper() == "LONG" else "BUY"


def _sanitize_comment_company(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return "UNK"
    out = []
    for ch in raw:
        if ch.isalnum():
            out.append(ch.upper())
        elif ch in {" ", "_", "-"}:
            out.append("_")
    compact = "".join(out).strip("_")
    return compact[:10] if compact else "UNK"


def _build_mt5_comment(trade: Dict[str, Any], trade_id: int) -> str:
    company = str(trade.get("company_name") or "").strip()
    if not company:
        try:
            company_id = int(trade.get("company_id") or 0)
        except Exception:
            company_id = 0
        if company_id > 0:
            try:
                company = str(get_company_name(company_id) or "").strip()
            except Exception:
                company = ""
    company_code = _sanitize_comment_company(company)

    grams = 0.0
    try:
        grams = abs(float(trade.get("weight") or 0.0))
    except Exception:
        grams = 0.0
    grams_text = f"{grams:.1f}g"
    # Keep comment short enough for MT5 (31 chars max on send).
    return f"HG{trade_id}_{company_code}_{grams_text}"


def _build_sl(entry: float, side: str, exposure_abs: float) -> float:
    # Fixed risk model: 0.5% stop from entry.
    sl_pct = max(0.0001, _num("HEDGE_SL_PCT", 0.005))
    dist = max(0.00001, abs(float(entry)) * sl_pct)
    if str(side).upper() == "BUY":
        return max(0.0, float(entry) - dist)
    return max(0.0, float(entry) + dist)


def _build_tp(entry: float, side: str) -> float:
    # Target RR 2:1 -> TP distance = 2 * SL distance.
    sl_pct = max(0.0001, _num("HEDGE_SL_PCT", 0.005))
    rr = max(1.0, _num("HEDGE_TP_RR", 2.0))
    dist = max(0.00001, abs(float(entry)) * sl_pct * rr)
    if str(side).upper() == "BUY":
        return max(0.0, float(entry) + dist)
    return max(0.0, float(entry) - dist)


def _estimate_usdzar_lot_from_usd_exposure(usd_exposure_abs: float) -> float:
    # 1.0 lot of USDZAR ~= 100,000 USD notional.
    min_lot = _num("HEDGE_USDZAR_MIN_LOT", 0.01)
    max_lot = _num("HEDGE_USDZAR_MAX_LOT", 2.0)
    contract_usd_per_lot = max(1.0, _num("HEDGE_USDZAR_USD_PER_LOT", 100000.0))
    proposed = max(min_lot, min(max_lot, float(usd_exposure_abs) / contract_usd_per_lot))
    return proposed


def _execute_usdzar_offset(
    trade: Dict[str, Any],
    trade_id: int,
    usd_exposure_abs: float,
    fallback_side: str,
    mt5_comment: str,
) -> Dict[str, Any]:
    enabled = _truthy("HEDGE_ENABLE_USDZAR_OFFSET", True)
    if not enabled:
        return {"ok": False, "skipped": True, "error": "usdzar_offset_disabled"}

    symbol = str(os.getenv("HEDGE_USDZAR_SYMBOL", "USDZAR") or "USDZAR").strip() or "USDZAR"
    side = str(fallback_side or "").upper()
    if side not in {"BUY", "SELL"}:
        side = "SELL"

    lot = _estimate_usdzar_lot_from_usd_exposure(usd_exposure_abs)
    tick = get_symbol_tick(symbol)
    if not tick.get("ok"):
        return {"ok": False, "error": str(tick.get("error") or "usdzar_tick_failed"), "symbol": symbol}

    entry_price = float(tick.get("ask") if side == "BUY" else tick.get("bid"))
    sl = _build_sl(entry_price, side, usd_exposure_abs)
    tp = _build_tp(entry_price, side)
    comment = f"{mt5_comment[:24]}_FX"

    return place_market_order(
        symbol=symbol,
        side=side,
        volume=lot,
        sl=sl,
        tp=tp,
        comment=comment,
    )


def _grams_to_ounces(grams: float) -> float:
    return max(0.0, float(grams)) / 31.1035


def _normalize_ai_decision(ai: Dict[str, Any], exposure_side: str) -> Dict[str, Any]:
    fallback = {
        "decision": "MARKET_NOW",
        "order_type": _default_side_for_exposure(exposure_side),
        "confidence": 0.4,
        "reason": "fallback",
        "entry_price": None,
        "take_profit_price": None,
        "trailing_activation_pct": 0.001,
        "notes": "",
    }
    if not bool(ai.get("ok")):
        d = dict(fallback)
        fb = ai.get("fallback") if isinstance(ai, dict) else None
        if isinstance(fb, dict):
            for key in ["decision", "confidence", "reason"]:
                if key in fb:
                    d[key] = fb[key]
        return d

    d = dict(fallback)
    parsed = ai.get("decision") if isinstance(ai.get("decision"), dict) else {}
    if isinstance(parsed, dict):
        d.update(parsed)

    side = _default_side_for_exposure(exposure_side)
    if str(d.get("order_type") or "").upper() not in {"BUY", "SELL"}:
        d["order_type"] = side
    if str(d.get("order_type") or "").upper() != side:
        d["order_type"] = side

    decision = str(d.get("decision") or "MARKET_NOW").upper()
    if decision not in {"MARKET_NOW", "PENDING_ORDER", "SKIP"}:
        decision = "MARKET_NOW"
    d["decision"] = decision
    return d


def _poll_new_trades() -> List[Dict[str, Any]]:
    min_seen = 0
    raw = _state_get("hedge_listener_last_seen_trade_id")
    if raw:
        try:
            min_seen = int(raw)
        except Exception:
            min_seen = 0

    res = fetch_trademc_trades(limit=120, offset=0, min_trade_id=min_seen, sort="id")
    if not isinstance(res, dict):
        return []
    rows = res.get("data")
    if not isinstance(rows, list):
        return []

    out = [r for r in rows if isinstance(r, dict) and r.get("id") is not None]
    out.sort(key=lambda r: int(r.get("id") or 0))
    # Safety valve: never process an unbounded backlog in one cycle.
    max_per_cycle = max(1, int(_num("HEDGE_MAX_TRADES_PER_CYCLE", 2)))
    if len(out) > max_per_cycle:
        out = out[-max_per_cycle:]

    if out:
        latest = int(out[-1].get("id") or min_seen)
        if latest > min_seen:
            _state_set("hedge_listener_last_seen_trade_id", str(latest))
    return out


def _bootstrap_last_seen_trade_id_if_needed() -> None:
    # Prevent historical backfill storms after first enable/restart.
    if not _truthy("HEDGE_SKIP_BACKLOG_ON_START", True):
        return
    current = _state_get("hedge_listener_last_seen_trade_id")
    if str(current or "").strip():
        return
    probe = fetch_trademc_trades(limit=1, offset=0, sort="-id")
    if not isinstance(probe, dict):
        return
    rows = probe.get("data")
    if not isinstance(rows, list) or not rows:
        return
    latest = int(rows[0].get("id") or 0)
    if latest > 0:
        _state_set("hedge_listener_last_seen_trade_id", str(latest))
        print(f"[HEDGE] bootstrap last_seen_trade_id={latest} (skip backlog enabled)")


def _execute_for_trade(trade: Dict[str, Any]) -> None:
    trade_id = int(trade.get("id") or 0)
    if trade_id <= 0:
        return
    if _event_exists_for_trade(trade_id):
        return

    exposure = _derive_exposure_side_and_value(trade)
    exposure_side = str(exposure["side"])
    abs_exp = float(exposure["abs_exposure"])
    symbol = str(os.getenv("HEDGE_MT5_SYMBOL", "XAUUSD") or "XAUUSD").strip() or "XAUUSD"

    event_id = _insert_event(
        {
            "trademc_trade_id": trade_id,
            "trade_ref": str(trade.get("ref_number") or ""),
            "event_type": "trade_detected",
            "event_status": "detected",
            "exposure_side": exposure_side,
            "exposure_value": float(exposure.get("signed_exposure") or 0.0),
            "symbol": symbol,
        }
    )

    mt5_login = mt5_connect()
    if not mt5_login.get("ok"):
        _update_event(event_id, {"event_status": "failed", "error_text": str(mt5_login.get("error") or "mt5_connect_failed")})
        return

    tick = get_symbol_tick(symbol)
    if not tick.get("ok"):
        _update_event(event_id, {"event_status": "failed", "error_text": str(tick.get("error") or "symbol_tick_failed")})
        mt5_shutdown()
        return

    ai_context = {
        "trade": {
            "id": trade_id,
            "status": trade.get("status"),
            "weight": trade.get("weight"),
            "ref_number": trade.get("ref_number"),
            "trade_timestamp": trade.get("trade_timestamp"),
        },
        "market": {
            "symbol": symbol,
            "bid": tick.get("bid"),
            "ask": tick.get("ask"),
            "last": tick.get("last"),
        },
        "exposure_side": exposure_side,
        "signed_exposure": exposure.get("signed_exposure"),
        "abs_exposure": abs_exp,
    }
    ai_raw = get_claude_hedge_decision(ai_context)
    ai = _normalize_ai_decision(ai_raw, exposure_side)
    mt5_comment = _build_mt5_comment(trade, trade_id)

    if str(ai.get("decision") or "") == "SKIP":
        _update_event(
            event_id,
            {
                "event_status": "skipped",
                "ai_payload_json": json.dumps(ai_raw, ensure_ascii=True),
                "error_text": str(ai.get("reason") or "ai_skip"),
            },
        )
        mt5_shutdown()
        return

    side = str(ai.get("order_type") or _default_side_for_exposure(exposure_side)).upper()
    # Size XAU leg from actual traded grams -> equivalent ounces.
    grams_abs = abs(float(trade.get("weight") or 0.0))
    xau_ounces = _grams_to_ounces(grams_abs)
    volume = estimate_lot_size_from_ounces(symbol, xau_ounces)
    if volume <= 0:
        volume = estimate_lot_size_from_exposure(symbol, abs_exp)
    market_entry = float(tick.get("ask") if side == "BUY" else tick.get("bid"))
    entry = ai.get("entry_price")
    try:
        entry_f = float(entry) if entry is not None else market_entry
    except Exception:
        entry_f = market_entry

    sl = _build_sl(entry_f if str(ai.get("decision")) == "PENDING_ORDER" else market_entry, side, abs_exp)
    tp_f = _build_tp(entry_f if str(ai.get("decision")) == "PENDING_ORDER" else market_entry, side)

    if str(ai.get("decision") or "").upper() == "PENDING_ORDER":
        result = place_pending_order(
            symbol=symbol,
            side=side,
            volume=volume,
            entry_price=entry_f,
            sl=sl,
            tp=tp_f,
            comment=mt5_comment,
        )
        mode = "pending"
    else:
        result = place_market_order(
            symbol=symbol,
            side=side,
            volume=volume,
            sl=sl,
            tp=tp_f,
            comment=mt5_comment,
        )
        mode = "market"

    if not result.get("ok"):
        _update_event(
            event_id,
            {
                "event_status": "failed",
                "execution_mode": mode,
                "hedge_order_type": side,
                "entry_price": entry_f,
                "stop_loss": sl,
                "take_profit": tp_f,
                "ai_payload_json": json.dumps(ai_raw, ensure_ascii=True),
                "error_text": str(result.get("error") or result.get("comment") or "mt5_order_failed"),
            },
        )
        mt5_shutdown()
        return

    position_ticket = int(result.get("deal") or 0)
    if position_ticket <= 0:
        # Pending order may not have an open position yet.
        position_ticket = int(result.get("order") or 0)

    _update_event(
        event_id,
        {
            "event_status": "placed" if mode == "market" else "pending",
            "execution_mode": mode,
            "hedge_order_type": side,
            "mt5_order_ticket": int(result.get("order") or 0),
            "mt5_position_ticket": position_ticket,
            "entry_price": float(result.get("price") or entry_f),
            "stop_loss": sl,
            "take_profit": tp_f,
            "trailing_enabled": 1,
            "trailing_distance": max(0.0, abs((entry_f - sl) if side == "BUY" else (sl - entry_f))),
            "ai_payload_json": json.dumps(ai_raw, ensure_ascii=True),
            "error_text": "",
        },
    )

    mt5_shutdown()


def _manage_trailing_stops() -> None:
    rows = _fetch_open_events()
    if not rows:
        return

    login = mt5_connect()
    if not login.get("ok"):
        return

    for row in rows:
        event_id = int(row["id"])
        symbol = str(row["symbol"] or "")
        side = str(row["hedge_order_type"] or "").upper()
        position_ticket = int(row["mt5_position_ticket"] or 0)
        trailing_distance = float(row["trailing_distance"] or 0.0)
        trade_id = int(row["trademc_trade_id"] or 0)
        trade_ref = str(row["trade_ref"] or "")
        if not symbol or side not in {"BUY", "SELL"} or position_ticket <= 0:
            continue

        pos = get_position_by_ticket(position_ticket)
        if pos is None:
            # Position is closed. Trigger USDZAR cutout once, using XAU executed notional.
            if trade_id > 0 and not _has_usdzar_offset_event(trade_id):
                close_summary = get_position_close_summary(position_ticket)
                if not close_summary.get("ok"):
                    _insert_event(
                        {
                            "trademc_trade_id": trade_id,
                            "trade_ref": trade_ref,
                            "event_type": "usdzar_offset",
                            "event_status": "failed",
                            "symbol": str(os.getenv("HEDGE_USDZAR_SYMBOL", "USDZAR") or "USDZAR"),
                            "error_text": str(close_summary.get("error") or "xau_close_not_confirmed"),
                        }
                    )
                    continue

                xau_contract_size = get_symbol_contract_size(symbol)
                if xau_contract_size <= 0:
                    xau_contract_size = 100.0
                xau_entry = float(row["entry_price"] or 0.0)
                xau_volume = 0.0
                try:
                    xau_volume = float(getattr(pos, "volume", 0.0) or 0.0)
                except Exception:
                    xau_volume = 0.0
                if xau_volume <= 0:
                    # Position already gone; use closed volume from deal history.
                    xau_volume = float(close_summary.get("closed_volume") or 0.0)
                usd_notional = abs(float(xau_volume) * float(xau_contract_size) * max(0.0, xau_entry))
                comment = f"HG{trade_id}_USD_CUT"
                usdzar_result = _execute_usdzar_offset(
                    trade={"company_name": "", "company_id": None, "weight": 0},
                    trade_id=trade_id,
                    usd_exposure_abs=usd_notional,
                    fallback_side=side,
                    mt5_comment=comment,
                )
                _insert_event(
                    {
                        "trademc_trade_id": trade_id,
                        "trade_ref": trade_ref,
                        "event_type": "usdzar_offset",
                        "event_status": "placed" if usdzar_result.get("ok") else "failed",
                        "symbol": str(os.getenv("HEDGE_USDZAR_SYMBOL", "USDZAR") or "USDZAR"),
                        "hedge_order_type": side,
                        "execution_mode": "market",
                        "mt5_order_ticket": int(usdzar_result.get("order") or 0),
                        "mt5_position_ticket": int(usdzar_result.get("deal") or usdzar_result.get("order") or 0),
                        "entry_price": float(usdzar_result.get("price") or 0.0),
                        "error_text": str(usdzar_result.get("error") or usdzar_result.get("comment") or ""),
                    }
                )
            continue

        current_sl = float(getattr(pos, "sl", 0.0) or 0.0)
        tick = get_symbol_tick(symbol)
        if not tick.get("ok"):
            continue

        bid = float(tick.get("bid") or 0.0)
        ask = float(tick.get("ask") or 0.0)
        dist = max(_num("HEDGE_MIN_SL_DISTANCE", 0.5), trailing_distance)

        if side == "BUY":
            proposed = max(0.0, bid - dist)
            better = proposed > current_sl if current_sl > 0 else proposed > 0
        else:
            proposed = max(0.0, ask + dist)
            better = proposed < current_sl if current_sl > 0 else proposed > 0

        if not better:
            continue

        upd = update_position_sl(symbol=symbol, position_ticket=position_ticket, new_sl=proposed, tp=float(row["take_profit"] or 0.0) or None)
        if upd.get("ok"):
            _update_event(event_id, {"stop_loss": proposed})

    mt5_shutdown()


_listener_lock = threading.Lock()
_listener_started = False


def hedging_listener_loop() -> None:
    poll_s = max(2.0, _num("HEDGE_LISTENER_POLL_SECONDS", 6.0))
    trailing_s = max(2.0, _num("HEDGE_TRAILING_CHECK_SECONDS", 5.0))
    last_trailing = 0.0

    print(f"[HEDGE] listener started | poll={poll_s}s trailing={trailing_s}s")
    while True:
        try:
            trades = _poll_new_trades()
            for trade in trades:
                try:
                    _execute_for_trade(trade)
                    time.sleep(max(0.0, _num("HEDGE_MIN_SECONDS_BETWEEN_ORDERS", 1.0)))
                except Exception as exc:
                    print(f"[HEDGE][ERROR] trade execution failed: {exc}")

            now = time.time()
            if now - last_trailing >= trailing_s:
                last_trailing = now
                try:
                    _manage_trailing_stops()
                except Exception as exc:
                    print(f"[HEDGE][ERROR] trailing manager failed: {exc}")

        except Exception as exc:
            print(f"[HEDGE][ERROR] listener loop failed: {exc}")

        time.sleep(poll_s)


def start_hedging_listener() -> None:
    global _listener_started
    if not _truthy("HEDGE_SERVICE_ENABLED", True):
        print("[HEDGE] service disabled (HEDGE_SERVICE_ENABLED=false)")
        return

    initialize_hedging_tables()
    _bootstrap_last_seen_trade_id_if_needed()
    with _listener_lock:
        if _listener_started:
            return
        thread = threading.Thread(target=hedging_listener_loop, daemon=True)
        thread.start()
        _listener_started = True
