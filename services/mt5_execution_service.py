import os
from typing import Any, Dict, Optional


def _lazy_mt5_module():
    try:
        import MetaTrader5 as mt5  # type: ignore
        return mt5
    except Exception:
        return None


def _num(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def mt5_connect() -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}

    login = _int("MT5_LOGIN", 0)
    password = str(os.getenv("MT5_PASSWORD", "") or "").strip()
    server = str(os.getenv("MT5_SERVER", "MetaQuotes-Demo") or "MetaQuotes-Demo").strip()
    terminal_path = str(os.getenv("MT5_TERMINAL_PATH", "") or "").strip()

    if login <= 0 or not password or not server:
        return {"ok": False, "error": "missing_mt5_credentials_in_env"}

    initialized = mt5.initialize(path=terminal_path) if terminal_path else mt5.initialize()
    if not initialized:
        return {"ok": False, "error": f"mt5_initialize_failed: {mt5.last_error()}"}

    authed = mt5.login(login=login, password=password, server=server)
    if not authed:
        return {"ok": False, "error": f"mt5_login_failed: {mt5.last_error()}"}

    return {"ok": True}


def mt5_shutdown() -> None:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return
    try:
        mt5.shutdown()
    except Exception:
        return


def get_symbol_tick(symbol: str) -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}
    if not mt5.symbol_select(symbol, True):
        return {"ok": False, "error": f"symbol_select_failed:{symbol}"}
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return {"ok": False, "error": f"symbol_tick_missing:{symbol}"}
    return {
        "ok": True,
        "bid": float(getattr(tick, "bid", 0.0) or 0.0),
        "ask": float(getattr(tick, "ask", 0.0) or 0.0),
        "last": float(getattr(tick, "last", 0.0) or 0.0),
        "time": int(getattr(tick, "time", 0) or 0),
    }


def _normalize_volume(symbol: str, volume: float) -> float:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return max(0.01, float(volume))
    info = mt5.symbol_info(symbol)
    if info is None:
        return max(0.01, float(volume))
    step = float(getattr(info, "volume_step", 0.01) or 0.01)
    vmin = float(getattr(info, "volume_min", 0.01) or 0.01)
    vmax = float(getattr(info, "volume_max", 100.0) or 100.0)
    vol = max(vmin, min(vmax, float(volume)))
    steps = round(vol / step)
    return max(vmin, min(vmax, steps * step))


def _send_with_filling_fallback(request: Dict[str, Any]) -> Any:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return None
    # Some symbols/brokers reject IOC/RETURN/FOK depending on contract config.
    for filling in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_RETURN, mt5.ORDER_FILLING_FOK):
        req = dict(request)
        req["type_filling"] = filling
        result = mt5.order_send(req)
        if result is None:
            continue
        retcode = int(getattr(result, "retcode", 0) or 0)
        # 10030: invalid filling mode -> try next.
        if retcode == 10030:
            continue
        return result
    # Final attempt with whatever was in request, preserving previous behavior.
    return mt5.order_send(request)


def estimate_lot_size_from_exposure(symbol: str, net_exposure_abs: float) -> float:
    # Conservative default sizing; overridable via env.
    min_lot = _num("HEDGE_MIN_LOT", 0.01)
    scale = _num("HEDGE_EXPOSURE_TO_LOT_SCALE", 500000.0)
    cap = _num("HEDGE_MAX_LOT", 2.0)
    proposed = max(min_lot, min(cap, float(net_exposure_abs) / max(1.0, scale)))
    return _normalize_volume(symbol, proposed)


def estimate_lot_size_from_ounces(symbol: str, ounces: float) -> float:
    mt5 = _lazy_mt5_module()
    oz = max(0.0, float(ounces))
    if oz <= 0:
        return _normalize_volume(symbol, _num("HEDGE_MIN_LOT", 0.01))
    contract_size = 100.0
    if mt5 is not None:
        info = mt5.symbol_info(symbol)
        if info is not None:
            try:
                contract_size = float(getattr(info, "trade_contract_size", 100.0) or 100.0)
            except Exception:
                contract_size = 100.0
    lots = oz / max(1.0, contract_size)
    return _normalize_volume(symbol, lots)


def get_symbol_contract_size(symbol: str) -> float:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return 0.0
    info = mt5.symbol_info(symbol)
    if info is None:
        return 0.0
    try:
        return float(getattr(info, "trade_contract_size", 0.0) or 0.0)
    except Exception:
        return 0.0


def place_market_order(symbol: str, side: str, volume: float, sl: Optional[float] = None, tp: Optional[float] = None, comment: str = "") -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}

    tick_res = get_symbol_tick(symbol)
    if not tick_res.get("ok"):
        return tick_res

    s = str(side or "").upper()
    if s not in {"BUY", "SELL"}:
        return {"ok": False, "error": f"invalid_side:{side}"}

    order_type = mt5.ORDER_TYPE_BUY if s == "BUY" else mt5.ORDER_TYPE_SELL
    price = float(tick_res["ask"] if s == "BUY" else tick_res["bid"])

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": _normalize_volume(symbol, volume),
        "type": order_type,
        "price": price,
        "deviation": _int("HEDGE_MT5_MAX_DEVIATION", 20),
        "magic": _int("HEDGE_MT5_MAGIC", 910001),
        "comment": (comment or "hedge_service")[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    if sl is not None and sl > 0:
        request["sl"] = float(sl)
    if tp is not None and tp > 0:
        request["tp"] = float(tp)

    result = _send_with_filling_fallback(request)
    if result is None:
        return {"ok": False, "error": f"order_send_failed:{mt5.last_error()}"}

    retcode = int(getattr(result, "retcode", 0) or 0)
    ok_codes = {10008, 10009}
    return {
        "ok": retcode in ok_codes,
        "retcode": retcode,
        "order": int(getattr(result, "order", 0) or 0),
        "deal": int(getattr(result, "deal", 0) or 0),
        "price": float(getattr(result, "price", 0.0) or 0.0),
        "volume": float(getattr(result, "volume", 0.0) or 0.0),
        "comment": str(getattr(result, "comment", "") or ""),
    }


def place_pending_order(symbol: str, side: str, volume: float, entry_price: float, sl: Optional[float] = None, tp: Optional[float] = None, comment: str = "") -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}

    tick_res = get_symbol_tick(symbol)
    if not tick_res.get("ok"):
        return tick_res

    bid = float(tick_res.get("bid") or 0.0)
    ask = float(tick_res.get("ask") or 0.0)
    s = str(side or "").upper()
    if s not in {"BUY", "SELL"}:
        return {"ok": False, "error": f"invalid_side:{side}"}

    px = float(entry_price)
    if px <= 0:
        return {"ok": False, "error": "invalid_entry_price"}

    if s == "BUY":
        otype = mt5.ORDER_TYPE_BUY_LIMIT if px < ask else mt5.ORDER_TYPE_BUY_STOP
    else:
        otype = mt5.ORDER_TYPE_SELL_LIMIT if px > bid else mt5.ORDER_TYPE_SELL_STOP

    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": _normalize_volume(symbol, volume),
        "type": otype,
        "price": px,
        "deviation": _int("HEDGE_MT5_MAX_DEVIATION", 20),
        "magic": _int("HEDGE_MT5_MAGIC", 910001),
        "comment": (comment or "hedge_pending")[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_RETURN,
    }
    if sl is not None and sl > 0:
        request["sl"] = float(sl)
    if tp is not None and tp > 0:
        request["tp"] = float(tp)

    result = _send_with_filling_fallback(request)
    if result is None:
        return {"ok": False, "error": f"order_send_failed:{mt5.last_error()}"}

    retcode = int(getattr(result, "retcode", 0) or 0)
    ok_codes = {10008, 10009}
    return {
        "ok": retcode in ok_codes,
        "retcode": retcode,
        "order": int(getattr(result, "order", 0) or 0),
        "deal": int(getattr(result, "deal", 0) or 0),
        "price": float(getattr(result, "price", 0.0) or 0.0),
        "volume": float(getattr(result, "volume", 0.0) or 0.0),
        "comment": str(getattr(result, "comment", "") or ""),
    }


def update_position_sl(symbol: str, position_ticket: int, new_sl: float, tp: Optional[float] = None) -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}
    if new_sl <= 0:
        return {"ok": False, "error": "invalid_new_sl"}

    req = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": symbol,
        "position": int(position_ticket),
        "sl": float(new_sl),
    }
    if tp is not None and tp > 0:
        req["tp"] = float(tp)

    result = mt5.order_send(req)
    if result is None:
        return {"ok": False, "error": f"order_send_failed:{mt5.last_error()}"}
    retcode = int(getattr(result, "retcode", 0) or 0)
    ok_codes = {10008, 10009}
    return {"ok": retcode in ok_codes, "retcode": retcode, "comment": str(getattr(result, "comment", "") or "")}


def get_position_by_ticket(position_ticket: int):
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return None
    positions = mt5.positions_get(ticket=int(position_ticket))
    if not positions:
        return None
    return positions[0]


def get_position_close_summary(position_ticket: int) -> Dict[str, Any]:
    mt5 = _lazy_mt5_module()
    if mt5 is None:
        return {"ok": False, "error": "metatrader5_package_not_installed"}
    try:
        deals = mt5.history_deals_get(position=int(position_ticket))
    except Exception as exc:
        return {"ok": False, "error": f"history_deals_get_failed:{exc}"}
    if not deals:
        return {"ok": False, "error": "no_deals_for_position"}

    total_profit = 0.0
    total_volume = 0.0
    has_close = False
    for d in deals:
        entry = int(getattr(d, "entry", 0) or 0)
        # 1 == DEAL_ENTRY_OUT (closed leg)
        if entry == 1:
            has_close = True
            total_profit += float(getattr(d, "profit", 0.0) or 0.0)
            total_profit += float(getattr(d, "swap", 0.0) or 0.0)
            total_profit += float(getattr(d, "commission", 0.0) or 0.0)
            total_volume += float(getattr(d, "volume", 0.0) or 0.0)

    if not has_close:
        return {"ok": False, "error": "position_not_closed_yet"}
    return {"ok": True, "realized_profit": total_profit, "closed_volume": total_volume}
