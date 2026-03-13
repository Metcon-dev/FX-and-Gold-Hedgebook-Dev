#!/usr/bin/env python
"""
Generate a PDF snapshot of the Open Positions Reval page sections:
1) Open unallocated positions + revaluated PnL
2) TradeMC daily buys and sells
3) Account balances recon table
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import math
import pathlib
import sys
from typing import Any, Dict, List, Tuple

from fpdf import FPDF


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        out = float(text)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _to_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _to_timestamp_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        if n <= 0:
            return None
        return int(n if n > 1e12 else n * 1000)
    text = _to_str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d", "%Y/%m/%d %H:%M:%S"):
        try:
            d = dt.datetime.strptime(text, fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=dt.timezone.utc)
            return int(d.timestamp() * 1000)
        except ValueError:
            pass
    try:
        d2 = dt.datetime.fromisoformat(text)
        if d2.tzinfo is None:
            d2 = d2.replace(tzinfo=dt.timezone.utc)
        return int(d2.timestamp() * 1000)
    except Exception:
        return None


def _compute_open_net_rows(rows: List[Dict[str, Any]], market: Dict[str, Any]) -> List[Dict[str, Any]]:
    market_xau = _to_float(market.get("xau_usd"))
    market_fx = _to_float(market.get("usd_zar"))
    out: List[Dict[str, Any]] = []
    for r in rows:
        pair_raw = _to_str(r.get("trade_num") or r.get("pair") or r.get("pair_symbol")).upper().replace("-", "/")
        fx_qty = _to_float(r.get("fx_qty_usd"))
        gold_qty = _to_float(r.get("gold_qty_oz"))

        if pair_raw in ("USD/ZAR", "USDZAR") and fx_qty is not None and abs(fx_qty) > 1e-9:
            out.append(
                {
                    "pair": "USD/ZAR",
                    "net_side": "LONG" if fx_qty > 0 else "SHORT",
                    "net_value": abs(fx_qty),
                    "wa_rate": _to_float(r.get("fx_wa_rate")),
                    "current_rate": _to_float(r.get("market_usd_zar")) or market_fx,
                    "pnl_zar": _to_float(r.get("fx_pnl_zar")),
                }
            )
        if pair_raw in ("XAU/USD", "XAUUSD") and gold_qty is not None and abs(gold_qty) > 1e-9:
            out.append(
                {
                    "pair": "XAU/USD",
                    "net_side": "LONG" if gold_qty > 0 else "SHORT",
                    "net_value": abs(gold_qty),
                    "wa_rate": _to_float(r.get("gold_wa_price")),
                    "current_rate": _to_float(r.get("market_xau_usd")) or market_xau,
                    "pnl_zar": _to_float(r.get("gold_pnl_zar")),
                }
            )
    rank = {"XAU/USD": 0, "USD/ZAR": 1}
    out.sort(key=lambda x: rank.get(_to_str(x.get("pair")), 99))
    return out


def _compute_trademc_daily_totals(trademc_rows: List[Dict[str, Any]], today_key: str) -> Dict[str, Any]:
    buy_total_g = 0.0
    sell_total_g = 0.0
    counted = 0

    for row in trademc_rows:
        ts = (
            row.get("trade_timestamp")
            or row.get("trade_date")
            or row.get("created_at")
            or row.get("timestamp")
            or row.get("date")
        )
        ts_ms = _to_timestamp_ms(ts)
        if not ts_ms:
            continue
        day = dt.datetime.utcfromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d")
        if day != today_key:
            continue

        weight = _to_float(row.get("weight") or row.get("Weight") or row.get("qty") or row.get("quantity") or row.get("Quantity"))
        if weight is None:
            continue
        side = _to_str(row.get("side") or row.get("Side") or row.get("trade_side") or row.get("trade_type") or row.get("type")).upper()
        abs_w = abs(weight)
        if side == "BUY":
            buy_total_g += abs_w
        elif side == "SELL":
            sell_total_g -= abs_w
        else:
            if weight >= 0:
                buy_total_g += weight
            else:
                sell_total_g += weight
        counted += 1

    return {
        "buy_total_g": buy_total_g,
        "sell_total_g": sell_total_g,
        "counted_trades": counted,
        "today_key": today_key,
    }


def _fmt_num(value: Any, dp: int) -> str:
    n = _to_float(value)
    if n is None:
        return "--"
    return f"{n:,.{dp}f}"


def _draw_section_title(pdf: FPDF, text: str) -> None:
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(0, 8, text, ln=1)
    pdf.ln(1)


def _draw_simple_table(pdf: FPDF, headers: List[str], rows: List[List[str]], widths: List[float]) -> None:
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(241, 245, 249)
    pdf.set_text_color(51, 65, 85)
    for i, h in enumerate(headers):
        pdf.cell(widths[i], 7, h, border=1, align="L", fill=True)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(15, 23, 42)
    for row in rows:
        for i, val in enumerate(row):
            align = "R" if i > 1 else "L"
            pdf.cell(widths[i], 7, _to_str(val, "--"), border=1, align=align)
        pdf.ln()


def _load_server_module(repo_root: pathlib.Path):
    server_py = repo_root / "j2-platform" / "server" / "server.py"
    spec = importlib.util.spec_from_file_location("j2_server_module", server_py)
    if not spec or not spec.loader:
        raise RuntimeError(f"Unable to load server module from {server_py}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["j2_server_module"] = module
    spec.loader.exec_module(module)
    return module


def _get_json(client, path: str) -> Any:
    resp = client.get(path)
    if resp.status_code != 200:
        raise RuntimeError(f"{path} failed with {resp.status_code}: {resp.get_data(as_text=True)[:300]}")
    try:
        return resp.get_json()
    except Exception:
        return json.loads(resp.get_data(as_text=True))


def generate_pdf(output_path: pathlib.Path, recon_start: str, recon_end: str) -> pathlib.Path:
    repo_root = pathlib.Path(__file__).resolve().parents[3]
    mod = _load_server_module(repo_root)
    app = mod.app

    with app.test_client() as client:
        reval = _get_json(client, "/api/pmx/open-positions-reval")
        tm_rows = _get_json(client, "/api/trademc/trades")
        recon = _get_json(client, f"/api/pmx/account-recon?start_date={recon_start}&end_date={recon_end}")

    rows = reval.get("rows") if isinstance(reval, dict) else []
    summary = reval.get("summary") if isinstance(reval, dict) else {}
    market = reval.get("market") if isinstance(reval, dict) else {}
    if not isinstance(rows, list):
        rows = []
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(market, dict):
        market = {}
    if not isinstance(tm_rows, list):
        tm_rows = []
    if not isinstance(recon, dict):
        recon = {}

    today_key = dt.date.today().strftime("%Y-%m-%d")
    open_rows = _compute_open_net_rows(rows, market)
    tm_daily = _compute_trademc_daily_totals(tm_rows, today_key)

    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 10, "Open Positions Reval Report", ln=1)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(71, 85, 105)
    pdf.cell(0, 6, f"Generated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=1)
    pdf.ln(2)

    _draw_section_title(pdf, "1) Open Unallocated Positions and Revaluated PnL")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"Open Pairs: {_fmt_num(summary.get('open_trades'), 0)}", ln=1)
    pdf.cell(0, 6, f"Current Gold ($/oz): {_fmt_num(market.get('xau_usd'), 4)}", ln=1)
    pdf.cell(0, 6, f"Current FX (USD/ZAR): {_fmt_num(market.get('usd_zar'), 5)}", ln=1)
    pdf.cell(0, 6, f"Total PnL (ZAR): {_fmt_num(summary.get('total_pnl_zar'), 2)}", ln=1)
    pdf.ln(2)

    open_headers = ["Pair", "Net Side", "Net Value", "Weighted Avg Rate", "Current Rate", "Current PnL (ZAR)"]
    open_widths = [40, 30, 45, 55, 45, 55]
    open_table_rows: List[List[str]] = []
    for r in open_rows:
        pair = _to_str(r.get("pair"))
        net_val = _to_float(r.get("net_value"))
        if pair == "USD/ZAR":
            net_val_txt = f"{_fmt_num(net_val, 2)} USD"
            wa_txt = f"R{_fmt_num(r.get('wa_rate'), 5)}"
            cur_txt = f"R{_fmt_num(r.get('current_rate'), 5)}"
        else:
            net_val_txt = f"{_fmt_num(net_val, 4)} oz"
            wa_txt = f"${_fmt_num(r.get('wa_rate'), 4)}/oz"
            cur_txt = f"${_fmt_num(r.get('current_rate'), 4)}/oz"
        open_table_rows.append(
            [
                pair,
                _to_str(r.get("net_side")),
                net_val_txt,
                wa_txt,
                cur_txt,
                f"R{_fmt_num(r.get('pnl_zar'), 2)}",
            ]
        )
    if not open_table_rows:
        open_table_rows = [["--", "--", "--", "--", "--", "--"]]
    _draw_simple_table(pdf, open_headers, open_table_rows, open_widths)
    pdf.ln(3)

    _draw_section_title(pdf, f"2) TradeMC Daily Buys and Sells ({tm_daily['today_key']})")
    tm_headers = ["Metric", "Value (g)"]
    tm_widths = [120, 60]
    tm_table_rows = [
        ["Daily Buys", _fmt_num(tm_daily.get("buy_total_g"), 2)],
        ["Daily Sells", _fmt_num(tm_daily.get("sell_total_g"), 2)],
        ["Daily Trades Counted", _fmt_num(tm_daily.get("counted_trades"), 0)],
    ]
    _draw_simple_table(pdf, tm_headers, tm_table_rows, tm_widths)
    pdf.ln(3)

    _draw_section_title(pdf, "3) Account Balances Recon Table")
    recon_headers = ["Currency", "Opening Balance", "Net Transactions", "Expected Balance", "Actual Balance", "Delta"]
    recon_widths = [40, 45, 50, 50, 50, 45]
    recon_rows_data: List[List[str]] = []
    currencies = [
        ("USD", "USD (LC)", 2),
        ("XAU", "XAU (oz)", 4),
        ("ZAR", "ZAR", 2),
    ]
    recon_map = recon.get("currencies") if isinstance(recon.get("currencies"), dict) else {}
    for key, label, dp in currencies:
        c = recon_map.get(key) if isinstance(recon_map.get(key), dict) else {}
        recon_rows_data.append(
            [
                label,
                _fmt_num(c.get("opening_balance"), dp),
                _fmt_num(c.get("transaction_total"), dp),
                _fmt_num(c.get("expected_balance"), dp),
                _fmt_num(c.get("actual_balance"), dp),
                _fmt_num(c.get("delta"), dp),
            ]
        )
    _draw_simple_table(pdf, recon_headers, recon_rows_data, recon_widths)
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(100, 116, 139)
    period_text = f"Statement period: {_to_str(recon.get('start_date'), recon_start)} -> {_to_str(recon.get('end_date'), recon_end)}"
    pdf.cell(0, 5, period_text, ln=1)
    if _to_str(recon.get("error")):
        pdf.cell(0, 5, f"Recon warning: {_to_str(recon.get('error'))}", ln=1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(output_path))
    return output_path


def main() -> int:
    today = dt.date.today().strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="Generate Open Positions Reval PDF report.")
    parser.add_argument("--start-date", default="2026-03-02", help="Recon start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=today, help="Recon end date (YYYY-MM-DD)")
    parser.add_argument("--output", default=f"Open_Positions_Reval_Report_{today}.pdf", help="Output PDF path")
    args = parser.parse_args()

    out_path = pathlib.Path(args.output)
    if not out_path.is_absolute():
        out_path = pathlib.Path(__file__).resolve().parents[3] / out_path

    result_path = generate_pdf(out_path, args.start_date, args.end_date)
    print(str(result_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
