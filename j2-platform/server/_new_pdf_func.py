"""
Trading Ticket PDF Generator — Metal Concentrators SA

Generates a professional trading ticket report using FPDF2.
Called from server.py via build_trading_ticket_pdf().
"""
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


def build_trading_ticket_pdf(
    trade_num_value: str,
    trademc_rows: pd.DataFrame,
    stonex_rows: pd.DataFrame,
    summary_rows: pd.DataFrame,
) -> Optional[bytes]:
    """Build a professional trading ticket PDF report.

    Returns PDF bytes, or None if FPDF is unavailable.
    """
    try:
        from fpdf import FPDF
    except Exception:
        return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _n(v: Any) -> Optional[float]:
        try:
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except Exception:
            return None

    def _t(v: Any) -> str:
        """Safe latin-1 text."""
        text = str(v if v is not None else "--")
        try:
            text.encode("latin-1")
            return text
        except Exception:
            return text.encode("latin-1", errors="replace").decode("latin-1")

    def _f(v: Any, dp: int = 2) -> str:
        n = _n(v)
        return "--" if n is None else f"{n:,.{dp}f}"

    def _money(v: Any, prefix: str = "$", dp: int = 2) -> str:
        n = _n(v)
        return "--" if n is None else f"{prefix}{n:,.{dp}f}"

    def _grams(v: Any) -> str:
        n = _n(v)
        return "--" if n is None else f"{n:,.2f} g"

    def _oz(v: Any, dp: int = 4) -> str:
        n = _n(v)
        return "--" if n is None else f"{n:,.{dp}f} oz"

    def _pct(v: Any, dp: int = 2) -> str:
        n = _n(v)
        return "--" if n is None else f"{n:,.{dp}f}%"

    TROY_OZ = 31.1035

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  COLOUR PALETTE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    CHARCOAL    = (28, 28, 28)
    DARK        = (45, 45, 45)
    MID         = (85, 85, 85)
    LIGHT       = (140, 140, 140)
    FAINT       = (180, 180, 180)

    GOLD        = (176, 120, 64)
    GOLD_DK     = (138, 92, 46)
    GOLD_LT     = (251, 245, 236)
    GOLD_MD     = (232, 213, 183)

    NAVY        = (26, 35, 50)
    CREAM       = (245, 237, 227)

    STRIPE      = (248, 248, 248)
    WHITE       = (255, 255, 255)
    BORDER      = (224, 224, 224)
    BORDER_SOFT = (238, 238, 238)

    GREEN       = (26, 122, 66)
    GREEN_BG    = (232, 245, 238)
    RED         = (192, 57, 43)
    RED_BG      = (253, 236, 235)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  DATA PREPARATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    tm_df = trademc_rows.copy() if isinstance(trademc_rows, pd.DataFrame) else pd.DataFrame()
    st_df = stonex_rows.copy() if isinstance(stonex_rows, pd.DataFrame) else pd.DataFrame()
    sum_df = summary_rows.copy() if isinstance(summary_rows, pd.DataFrame) else pd.DataFrame()

    # -- TradeMC bookings --
    bookings: List[Dict[str, Any]] = []
    tm_tot = {"g": 0.0, "oz": 0.0, "usd": 0.0, "zar_gross": 0.0, "zar_net": 0.0}
    for _, r in tm_df.iterrows():
        wg = _n(r.get("Weight (g)")) or 0.0
        woz = _n(r.get("Weight (oz)")) or (wg / TROY_OZ)
        bp = _n(r.get("$/oz Booked"))
        fx = _n(r.get("FX Rate"))
        usd = _n(r.get("USD Value"))
        if usd is None and bp is not None:
            usd = woz * bp
        zg = _n(r.get("ZAR Value"))
        if zg is None and usd is not None and fx is not None:
            zg = usd * fx
        rr = _n(r.get("company_refining_rate")) or 0.0
        rd = (zg * rr / 100.0) if zg is not None else None
        zn = _n(r.get("zar_value_less_refining"))
        if zn is None and zg is not None:
            zn = zg * (1.0 - rr / 100.0)
        company = str(r.get("Company") or r.get("company_name") or "Unknown")
        bookings.append({
            "company": company, "wg": wg, "woz": woz, "bp": bp, "fx": fx,
            "usd": usd, "zg": zg, "rr": rr, "rd": rd, "zn": zn,
        })
        tm_tot["g"] += wg
        tm_tot["oz"] += woz
        tm_tot["usd"] += usd or 0.0
        tm_tot["zar_gross"] += zg or 0.0
        tm_tot["zar_net"] += zn or 0.0

    # -- StoneX trades --
    def _sym(v: Any) -> str:
        return str(v or "").upper().replace("/", "").replace("-", "").replace(" ", "")

    xau_trades: List[Dict] = []
    fx_trades: List[Dict] = []
    all_pmx: List[Dict] = []
    for _, r in st_df.iterrows():
        sym = _sym(r.get("Symbol"))
        side = str(r.get("Side", "")).upper().strip()
        qty = abs(_n(r.get("Quantity")) or 0.0)
        price = _n(r.get("Price"))
        if qty <= 0:
            continue
        notional = (qty * price) if price is not None else None
        info = {
            "sym": sym, "side": side, "qty": qty, "price": price, "notional": notional,
            "trade_date": str(r.get("Trade Date") or ""),
            "value_date": str(r.get("Value Date") or ""),
            "fnc": str(r.get("FNC #") or ""),
            "doc": str(r.get("Doc #") or ""),
            "narration": str(r.get("Narration") or ""),
            "settle_ccy": str(r.get("Settle Currency") or ""),
            "settle_amt": _n(r.get("Settle Amount")),
        }
        all_pmx.append(info)
        if sym == "XAUUSD":
            xau_trades.append(info)
        elif sym == "USDZAR":
            fx_trades.append(info)

    # Weighted averages
    gold_tot_not = sum(t["notional"] or 0 for t in xau_trades)
    gold_tot_qty = sum(t["qty"] for t in xau_trades)
    gold_wa = (gold_tot_not / gold_tot_qty) if gold_tot_qty > 1e-9 else None

    fx_tot_not = sum(t["notional"] or 0 for t in fx_trades)
    fx_tot_qty = sum(t["qty"] for t in fx_trades)
    fx_wa = (fx_tot_not / fx_tot_qty) if fx_tot_qty > 1e-9 else None

    spot_zar_g = (gold_wa * fx_wa) / TROY_OZ if gold_wa and fx_wa else None

    # Cash flows
    xau_cfs: List[Dict] = []
    net_usd = 0.0
    for t in xau_trades:
        signed = None
        if t["notional"] is not None:
            signed = t["notional"] if t["side"] == "SELL" else -t["notional"]
        xau_cfs.append({**t, "signed": signed})
        net_usd += signed or 0.0

    # Summary
    base = sum_df.iloc[0].to_dict() if not sum_df.empty else {}
    sell_usd = _n(base.get("Sell Side (USD)"))
    buy_usd = _n(base.get("Buy Side (USD)"))
    sell_zar = _n(base.get("Sell Side (ZAR)"))
    buy_zar = _n(base.get("Buy Side (ZAR)"))
    profit_usd = _n(base.get("Profit (USD)"))
    profit_zar = _n(base.get("Profit (ZAR)"))
    profit_pct = _n(base.get("Profit % (ZAR Spot Cost)"))
    ctrl_g = _n(base.get("Control Account (g)"))
    ctrl_oz = _n(base.get("Control Account (oz)"))
    ctrl_zar = _n(base.get("Control Account (ZAR)"))
    traded_g = _n(base.get("Total Traded (g)"))
    traded_oz = _n(base.get("Total Traded (oz)"))
    stx_zar_flow = _n(base.get("StoneX ZAR Flow"))
    s_gold_wa = _n(base.get("Gold WA $/oz"))
    s_fx_wa = _n(base.get("FX WA USD/ZAR"))
    s_spot = _n(base.get("Spot ZAR/g"))

    gold_wa_d = s_gold_wa if s_gold_wa is not None else gold_wa
    fx_wa_d = s_fx_wa if s_fx_wa is not None else fx_wa
    spot_d = s_spot if s_spot is not None else spot_zar_g

    if profit_zar is None and sell_zar is not None and buy_zar is not None:
        profit_zar = sell_zar - buy_zar
    if profit_usd is None and sell_usd is not None and buy_usd is not None:
        profit_usd = sell_usd - buy_usd
    if profit_pct is None and profit_zar is not None and buy_zar and abs(buy_zar) > 1e-12:
        profit_pct = (profit_zar / buy_zar) * 100.0
    if ctrl_oz is None and ctrl_g is not None:
        ctrl_oz = ctrl_g / TROY_OZ
    if traded_oz is None and traded_g is not None:
        traded_oz = traded_g / TROY_OZ
    stonex_g = traded_g or (traded_oz * TROY_OZ if traded_oz else None)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PDF SETUP
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    pdf = FPDF("L", "mm", "A4")
    pdf.set_auto_page_break(True, margin=16)
    LM = 16.0
    RM = 16.0
    PW = 297.0
    W = PW - LM - RM
    now_str = datetime.now().strftime("%d %B %Y  %H:%M")

    # ── Page footer via alias_nb_pages ───────────────────────────
    class TicketPDF(FPDF):
        def footer(self):
            self.set_y(-12)
            self.set_draw_color(*GOLD_MD)
            self.set_line_width(0.2)
            self.line(LM, self.get_y(), PW - RM, self.get_y())
            self.set_y(-10)
            self.set_font("Arial", "", 6)
            self.set_text_color(*LIGHT)
            self.cell(W / 2, 4, _t(f"Metal Concentrators SA  |  Confidential  |  Trade #{trade_num_value}"),
                      0, 0, "L")
            self.cell(W / 2, 4, _t(f"Generated {now_str}  |  Page {self.page_no()}/{{nb}}"),
                      0, 0, "R")

    pdf = TicketPDF("L", "mm", "A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(True, margin=16)
    pdf.set_left_margin(LM)
    pdf.set_right_margin(RM)
    pdf.add_page()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  LAYOUT COMPONENTS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _check_space(needed: float = 25.0):
        """Add page if not enough vertical space."""
        if pdf.get_y() > (210 - 16 - needed):
            pdf.add_page()

    def _header_block():
        """Page header with logo, title, and double rule."""
        logo = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "MetCon Logo.png",
        )
        if os.path.isfile(logo):
            try:
                pdf.image(logo, x=LM, y=10, w=48, h=12)
            except Exception:
                pass

        # Right-aligned title block
        rx = PW / 2
        rw = PW / 2 - RM
        pdf.set_xy(rx, 10)
        pdf.set_text_color(*GOLD)
        pdf.set_font("Arial", "B", 6.8)
        pdf.cell(rw, 3.5, _t("METAL CONCENTRATORS SA"), 0, 2, "R")
        pdf.set_text_color(*CHARCOAL)
        pdf.set_font("Arial", "B", 16)
        pdf.cell(rw, 6.8, _t("Trading Ticket Report"), 0, 2, "R")
        pdf.set_text_color(*GOLD)
        pdf.set_font("Arial", "B", 9.8)
        pdf.cell(rw, 4.2, _t(f"Trade #{trade_num_value}"), 0, 2, "R")
        pdf.set_text_color(*LIGHT)
        pdf.set_font("Arial", "", 6.5)
        pdf.cell(rw, 3.5, _t(f"Generated {now_str}"), 0, 1, "R")

        y = pdf.get_y() + 1.6
        pdf.set_draw_color(*GOLD)
        pdf.set_line_width(0.5)
        pdf.line(LM, y, LM + W, y)
        pdf.set_draw_color(*GOLD_MD)
        pdf.set_line_width(0.15)
        pdf.line(LM, y + 1, LM + W, y + 1)
        pdf.set_y(y + 4.4)

    def _section(title: str, subtitle: str = ""):
        """Section header with clean accent and softer spacing."""
        _check_space(20)
        y = pdf.get_y()
        h = 8.5
        pdf.set_fill_color(*GOLD_LT)
        pdf.rect(LM, y, W, h, style="F")
        pdf.set_fill_color(*GOLD)
        pdf.rect(LM, y, 2.6, h, style="F")
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.12)
        pdf.rect(LM, y, W, h, style="D")
        pdf.set_xy(LM + 6, y + 1.9)
        pdf.set_text_color(*CHARCOAL)
        pdf.set_font("Arial", "B", 9.2)
        pdf.cell(W - 8, 4, _t(title), 0, 1, "L")
        pdf.set_y(y + h + 1.2)
        if subtitle:
            pdf.set_text_color(*LIGHT)
            pdf.set_font("Arial", "", 6.6)
            pdf.multi_cell(W, 3.5, _t(subtitle), 0, "L")
        pdf.ln(2.2)

    def _step(num: str, title: str, subtitle: str = ""):
        """Numbered audit step header."""
        _check_space(20)
        y = pdf.get_y()
        badge_w = 16
        pdf.set_fill_color(*GOLD_DK)
        pdf.rect(LM, y, badge_w, 6.6, style="F")
        pdf.set_xy(LM, y + 1.0)
        pdf.set_text_color(*WHITE)
        pdf.set_font("Arial", "B", 8.5)
        pdf.cell(badge_w, 4.2, _t(num), 0, 0, "C")
        pdf.set_xy(LM + badge_w + 3.4, y + 0.7)
        pdf.set_text_color(*CHARCOAL)
        pdf.set_font("Arial", "B", 9.4)
        pdf.cell(W - badge_w - 3.4, 5, _t(title), 0, 1, "L")
        pdf.set_y(y + 7.2)
        if subtitle:
            pdf.set_text_color(*MID)
            pdf.set_font("Arial", "", 6.6)
            pdf.multi_cell(W, 3.5, _t(subtitle), 0, "L")
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.1)
        pdf.line(LM, pdf.get_y() + 1.0, LM + W, pdf.get_y() + 1.0)
        pdf.ln(3.8)

    def _kv(label: str, value: str, bold_val: bool = True,
            val_color=None, indent: int = 0):
        """Key-value row with cleaner spacing."""
        _check_space(6.5)
        vc = val_color or DARK
        lpad = LM + indent * 12
        label_w = W * 0.55 - indent * 12
        val_w = W * 0.45

        pdf.set_text_color(*MID)
        pdf.set_font("Arial", "", 7.2)
        pdf.set_x(lpad)
        pdf.cell(label_w, 4.8, _t(label), 0, 0, "L")

        pdf.set_text_color(*vc)
        pdf.set_font("Arial", "B" if bold_val else "", 7.4)
        pdf.cell(val_w, 4.8, _t(value), 0, 1, "R")
        pdf.ln(0.35)

    def _formula(label: str, formula: str, result: str,
                 val_color=None, indent: int = 0):
        """Three-part row: label | formula -> | result."""
        _check_space(6.5)
        vc = val_color or DARK
        lpad = LM + indent * 12
        lw = W * 0.28 - indent * 12
        fw = W * 0.42
        rw = W * 0.30

        pdf.set_text_color(*MID)
        pdf.set_font("Arial", "", 7.0)
        pdf.set_x(lpad)
        pdf.cell(lw, 4.8, _t(label), 0, 0, "L")

        pdf.set_text_color(*LIGHT)
        pdf.set_font("Arial", "I", 6.0)
        pdf.cell(fw, 4.8, _t(f"{formula}  ->"), 0, 0, "R")

        pdf.set_text_color(*vc)
        pdf.set_font("Arial", "B", 7.4)
        pdf.cell(rw, 4.8, _t(result), 0, 1, "R")
        pdf.ln(0.35)

    def _sub_title(title: str):
        """Sub-section title with left accent."""
        _check_space(9)
        y = pdf.get_y()
        pdf.set_fill_color(*GOLD)
        pdf.rect(LM, y, 2, 5.6, style="F")
        pdf.set_fill_color(247, 241, 232)
        pdf.rect(LM + 2, y, W - 2, 5.6, style="F")
        pdf.set_xy(LM + 6, y + 1.1)
        pdf.set_text_color(*DARK)
        pdf.set_font("Arial", "B", 7.3)
        pdf.cell(W - 8, 3.4, _t(title), 0, 1, "L")
        pdf.set_y(y + 6.6)

    def _table(headers: List[str], rows: List[List[str]],
               col_pcts: List[float], right_cols: Optional[List[int]] = None):
        """Clean data table with compact rows and readable striping."""
        right_cols = right_cols or []
        widths = [W * p for p in col_pcts]

        if not rows:
            pdf.set_text_color(*LIGHT)
            pdf.set_font("Arial", "I", 7.5)
            pdf.cell(W, 5, _t("No data available."), 0, 1, "L")
            pdf.ln(3)
            return

        def _draw_header():
            _check_space(12)
            pdf.set_fill_color(*DARK)
            pdf.set_text_color(*WHITE)
            pdf.set_font("Arial", "B", 6.4)
            for i, h in enumerate(headers):
                align = "R" if i in right_cols else "C"
                pdf.cell(widths[i], 6.6, _t(h), 0, 0, align, True)
            pdf.ln()
            pdf.set_draw_color(*GOLD)
            pdf.set_line_width(0.32)
            pdf.line(LM, pdf.get_y(), LM + W, pdf.get_y())
            pdf.ln(0.3)

        _draw_header()
        for ri, row in enumerate(rows):
            if pdf.get_y() > 190:
                pdf.add_page()
                _draw_header()
            bg = WHITE if ri % 2 == 0 else STRIPE
            pdf.set_fill_color(*bg)
            pdf.set_text_color(*DARK)
            pdf.set_font("Arial", "", 6.9)
            for ci, cell in enumerate(row):
                align = "R" if ci in right_cols else "L"
                col_name = str(headers[ci]).strip().upper() if ci < len(headers) else ""
                # Truncate long text, but preserve audit identifiers like FNC numbers.
                if col_name in {"FNC #", "DOC #"}:
                    max_chars = max(18, int(widths[ci] / 1.35))
                else:
                    max_chars = max(6, int(widths[ci] / 1.92))
                txt = _t(cell)
                if len(txt) > max_chars:
                    txt = txt[:max_chars - 3] + "..."
                pdf.cell(widths[ci], 5.4, txt, 0, 0, align, True)
            pdf.ln()
            pdf.set_draw_color(*BORDER_SOFT)
            pdf.set_line_width(0.05)
            pdf.line(LM, pdf.get_y(), LM + W, pdf.get_y())
        pdf.ln(2.8)

    def _totals_table(headers: List[str], row: List[str],
                      col_pcts: List[float], right_cols: Optional[List[int]] = None):
        """Single totals row with gold background."""
        right_cols = right_cols or []
        widths = [W * p for p in col_pcts]
        pdf.set_draw_color(*GOLD)
        pdf.set_line_width(0.3)
        pdf.line(LM, pdf.get_y(), LM + W, pdf.get_y())
        pdf.ln(0.3)
        pdf.set_fill_color(246, 239, 228)
        pdf.set_text_color(*CHARCOAL)
        pdf.set_font("Arial", "B", 7.0)
        for ci, cell in enumerate(row):
            align = "R" if ci in right_cols else "L"
            pdf.cell(widths[ci], 6.2, _t(cell), 0, 0, align, True)
        pdf.ln()
        pdf.set_draw_color(*GOLD)
        pdf.line(LM, pdf.get_y(), LM + W, pdf.get_y())
        pdf.ln(3.5)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PAGE 1 — HEADER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    _header_block()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  EXECUTIVE SUMMARY — KPI Cards
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    _section("Executive Summary",
             "Key performance indicators for this trade at a glance.")

    def _kpi_card(x: float, y: float, width: float, label: str, value: str,
                  accent=GOLD, val_color=CHARCOAL, bg=WHITE):
        h = 19.5
        # Background
        pdf.set_fill_color(*bg)
        pdf.rect(x, y, width, h, style="F")
        # Top accent
        pdf.set_fill_color(*accent)
        pdf.rect(x, y, width, 1.8, style="F")
        # Border
        pdf.set_draw_color(*BORDER)
        pdf.set_line_width(0.15)
        pdf.rect(x, y, width, h, style="D")
        # Label
        pdf.set_xy(x, y + 2.8)
        pdf.set_text_color(*LIGHT)
        pdf.set_font("Arial", "B", 5.8)
        pdf.cell(width, 3, _t(label.upper()), 0, 0, "C")
        # Value
        pdf.set_xy(x, y + 8.3)
        pdf.set_text_color(*val_color)
        pdf.set_font("Arial", "B", 10.8)
        pdf.cell(width, 5, _t(value), 0, 0, "C")

    card_w = (W - 12) / 4
    card_gap = 4
    p_clr = GREEN if (profit_zar is not None and profit_zar >= 0) else RED
    p_bg = GREEN_BG if (profit_zar is not None and profit_zar >= 0) else RED_BG

    kpi_y = pdf.get_y()
    _kpi_card(LM, kpi_y, card_w, "Total Weight Traded", _grams(stonex_g))
    _kpi_card(LM + card_w + card_gap, kpi_y, card_w, "Gold WA Price",
              (_money(gold_wa_d, "$", 3) + "/oz") if gold_wa_d else "--")
    _kpi_card(LM + 2 * (card_w + card_gap), kpi_y, card_w, "Weighted Avg FX",
              f"R {fx_wa_d:,.4f}" if fx_wa_d else "--")
    _kpi_card(LM + 3 * (card_w + card_gap), kpi_y, card_w, "Net Profit (ZAR)",
              _money(profit_zar, "R ", 2), accent=p_clr, val_color=p_clr, bg=p_bg)
    pdf.set_y(kpi_y + 22.2)

    # Secondary metrics
    sec: List[tuple] = []
    if spot_d is not None:
        sec.append(("Implied Spot ZAR/g", f"R {spot_d:,.4f}"))
    if stx_zar_flow is not None:
        sec.append(("StoneX ZAR Inflow", _money(stx_zar_flow, "R ", 2)))
    if sell_usd is not None:
        sec.append(("Sell Side (USD)", _money(sell_usd, "$", 2)))
    if buy_usd is not None:
        sec.append(("Buy Side (USD)", _money(buy_usd, "$", 2)))
    if sell_zar is not None:
        sec.append(("Sell Side (ZAR)", _money(sell_zar, "R ", 2)))
    if buy_zar is not None:
        sec.append(("Buy Side (ZAR)", _money(buy_zar, "R ", 2)))
    if profit_usd is not None:
        sec.append(("Profit (USD)", _money(profit_usd, "$", 2)))
    if profit_pct is not None:
        sec.append(("Profit Margin", _pct(profit_pct, 3)))
    if ctrl_g is not None:
        sec.append(("Control Account", _grams(ctrl_g)))
    if ctrl_zar is not None:
        sec.append(("Control (ZAR)", _money(ctrl_zar, "R ", 2)))

    if sec:
        per_row = min(len(sec), 6)
        cell_w = W / per_row
        for chunk_i in range(0, len(sec), per_row):
            chunk = sec[chunk_i:chunk_i + per_row]
            n = len(chunk)
            cw = W / n
            y = pdf.get_y()
            pdf.set_fill_color(*STRIPE)
            pdf.rect(LM, y, W, 10, style="F")
            pdf.set_draw_color(*BORDER)
            pdf.set_line_width(0.1)
            pdf.rect(LM, y, W, 10, style="D")
            for i, (lbl, val) in enumerate(chunk):
                x = LM + i * cw
                if i > 0:
                    pdf.line(x, y, x, y + 10)
                pdf.set_xy(x, y + 1)
                pdf.set_text_color(*LIGHT)
                pdf.set_font("Arial", "B", 5)
                pdf.cell(cw, 3, _t(lbl.upper()), 0, 0, "C")
                pdf.set_xy(x, y + 4.5)
                pdf.set_text_color(*DARK)
                pdf.set_font("Arial", "B", 8)
                pdf.cell(cw, 4, _t(val), 0, 0, "C")
            pdf.set_y(y + 11.5)
    pdf.ln(4)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CLIENT BOOKINGS TABLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    _section("Client Bookings - Buy Side",
             "Gold purchased from TradeMC clients, valued in USD and "
             "converted to ZAR with refining deductions applied.")

    tm_headers = ["Company", "Weight (g)", "Weight (oz)", "$/oz Booked",
                   "FX Rate", "USD Value", "ZAR Gross", "Refining %", "ZAR Net"]
    tm_pcts = [0.17, 0.09, 0.09, 0.10, 0.09, 0.11, 0.12, 0.09, 0.14]
    tm_right = list(range(1, 9))

    tm_rows = []
    for b in bookings:
        tm_rows.append([
            _t(b["company"]), _f(b["wg"]), _f(b["woz"], 4), _f(b["bp"]),
            _f(b["fx"], 4), _f(b["usd"]), _f(b["zg"]),
            _pct(b["rr"]), _f(b["zn"]),
        ])
    _table(tm_headers, tm_rows, tm_pcts, right_cols=tm_right)

    if bookings:
        _totals_table(tm_headers, [
            "TOTAL", _f(tm_tot["g"]), _f(tm_tot["oz"], 4), "", "",
            _f(tm_tot["usd"]), _f(tm_tot["zar_gross"]), "",
            _f(tm_tot["zar_net"]),
        ], tm_pcts, right_cols=tm_right)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  STONEX / PMX TRADES TABLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    _section("StoneX / PMX Trades - Sell Side",
             "Gold sold and FX hedged through StoneX, generating USD "
             "and ZAR proceeds to settle client bookings.")

    stx_all_cols = ["Doc #", "FNC #", "Trade Date", "Value Date", "Symbol",
                    "Side", "Narration", "Quantity", "Price",
                    "Settle Currency", "Settle Amount"]
    stx_avail = [c for c in stx_all_cols if c in st_df.columns]
    _sw_map = {
        "Doc #": 0.07, "FNC #": 0.10, "Trade Date": 0.08, "Value Date": 0.08,
        "Symbol": 0.06, "Side": 0.05, "Narration": 0.17, "Quantity": 0.10,
        "Price": 0.09, "Settle Currency": 0.06, "Settle Amount": 0.10,
    }
    sw_sum = sum(_sw_map.get(c, 0.08) for c in stx_avail) or 1
    stx_pcts = [_sw_map.get(c, 0.08) / sw_sum for c in stx_avail]
    stx_right = [i for i, c in enumerate(stx_avail)
                 if c in ("Quantity", "Price", "Settle Amount")]

    stx_rows = []
    for _, r in st_df.iterrows():
        row = []
        doc_val_raw = _t(r.get("Doc #", "--"))
        fnc_val_raw = _t(r.get("FNC #", "--"))
        doc_upper = str(doc_val_raw).strip().upper()
        fnc_upper = str(fnc_val_raw).strip().upper()
        for c in stx_avail:
            if c == "Quantity":
                row.append(_f(r.get(c), 2))
            elif c == "Price":
                row.append(_f(r.get(c), 4))
            elif c == "Settle Amount":
                row.append(_f(r.get(c), 2))
            elif c == "Doc #":
                # Avoid duplicate audit refs when PMX stores FNC values in both Doc# and FNC#.
                if doc_upper.startswith("FNC/") and fnc_upper.startswith("FNC/"):
                    row.append("--")
                else:
                    row.append(doc_val_raw)
            else:
                row.append(_t(r.get(c, "--")))
        stx_rows.append(row)
    _table(stx_avail, stx_rows, stx_pcts, right_cols=stx_right)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  AUDIT TRAIL — Page Break
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    pdf.add_page()

    # Audit trail header
    pdf.set_text_color(*CHARCOAL)
    pdf.set_font("Arial", "B", 16)
    pdf.cell(W, 7, _t("Detailed Calculation Audit Trail"), 0, 1, "L")
    pdf.set_text_color(*GOLD)
    pdf.set_font("Arial", "B", 8)
    pdf.cell(0, 4, _t(f"Trade #{trade_num_value}"), 0, 0, "L")
    pdf.set_text_color(*LIGHT)
    pdf.set_font("Arial", "", 7)
    pdf.cell(0, 4, _t("   -  Step-by-step breakdown for reconciliation and verification."), 0, 1, "L")
    pdf.ln(1)
    y = pdf.get_y()
    pdf.set_draw_color(*GOLD)
    pdf.set_line_width(0.6)
    pdf.line(LM, y, LM + W, y)
    pdf.set_draw_color(*GOLD_MD)
    pdf.set_line_width(0.15)
    pdf.line(LM, y + 1, LM + W, y + 1)
    pdf.set_y(y + 5)

    # ── Step 1 — Source Data Overview ────────────────────────────
    _step("1", "Source Data Overview",
          "Counts of raw data loaded from TradeMC (buy side) and StoneX/PMX (sell side).")
    _kv("TradeMC client bookings (buy side)", str(len(bookings)))
    _kv("StoneX/PMX execution trades (sell side)", str(len(all_pmx)))
    _kv("XAU/USD gold price trades", str(len(xau_trades)), indent=1)
    _kv("USD/ZAR foreign exchange trades", str(len(fx_trades)), indent=1)
    _kv("Troy ounce conversion factor", "1 troy oz = 31.1035 grams")
    pdf.ln(4)

    # ── Step 2 — Client Booking Valuation ────────────────────────
    _step("2", "Client Booking Valuation (Buy Side)",
          "Each booking: grams -> oz, priced in USD, converted to ZAR, less refining.")

    for idx, b in enumerate(bookings):
        _sub_title(f"Booking {idx + 1}: {b['company']}")
        _kv("Raw weight received", _grams(b["wg"]), indent=1)
        _formula("Troy ounces", f"{_f(b['wg'])} g / 31.1035", _oz(b["woz"], 6), indent=1)
        _kv("Booked gold price", _money(b["bp"], "$") + " /oz", indent=1)
        _formula("USD value", f"{_oz(b['woz'], 6)} x {_money(b['bp'], '$')}", _money(b["usd"], "$"), indent=1)
        _kv("FX rate (ZAR/USD)", _f(b["fx"], 4), indent=1)
        _formula("ZAR gross", f"{_money(b['usd'], '$')} x {_f(b['fx'], 4)}", _money(b["zg"], "R "), indent=1)
        _kv("Refining rate", _pct(b["rr"]), indent=1)
        _formula("Refining deduction", f"{_money(b['zg'], 'R ')} x {_pct(b['rr'])}", _money(b["rd"], "R "), indent=1)
        _kv("ZAR net (after refining)", _money(b["zn"], "R "), val_color=GOLD_DK, indent=1)

    _sub_title("Aggregated Buy Side Totals")
    _kv("Combined weight", f"{_grams(tm_tot['g'])} ({_oz(tm_tot['oz'], 6)})")
    _kv("Combined USD value", _money(tm_tot["usd"], "$"))
    _kv("Combined ZAR gross", _money(tm_tot["zar_gross"], "R "))
    _kv("Combined ZAR net", _money(tm_tot["zar_net"], "R "), val_color=GOLD_DK)
    pdf.ln(4)

    # ── Step 3 — Weighted Average Pricing ────────────────────────
    _step("3", "Weighted Average Price Calculation",
          "WA = Sum(qty x price) / Sum(qty). Larger trades carry more weight.")

    _sub_title("Gold Weighted Average (XAU/USD)")
    if xau_trades:
        for i, t in enumerate(xau_trades):
            _formula(
                f"Trade {i+1}: {t['side']} {_oz(t['qty'], 4)} @ {_money(t['price'], '$', 4)}",
                f"{_oz(t['qty'], 4)} x {_money(t['price'], '$', 4)}",
                _money(t["notional"], "$"), indent=1,
            )
        _kv("Total notional", _money(gold_tot_not, "$"))
        _kv("Total quantity", _oz(gold_tot_qty, 4))
        _formula("Gold WA", f"{_money(gold_tot_not, '$')} / {_oz(gold_tot_qty, 4)}",
                 _money(gold_wa_d, "$", 4), val_color=GOLD)
    else:
        _kv("No XAU/USD trades found", "--")
    pdf.ln(2)

    _sub_title("FX Weighted Average (USD/ZAR)")
    if fx_trades:
        for i, t in enumerate(fx_trades):
            _formula(
                f"Trade {i+1}: {t['side']} {_money(t['qty'], '$', 2)} @ {_money(t['price'], 'R ', 4)}",
                f"{_money(t['qty'], '$', 2)} x {_money(t['price'], 'R ', 4)}",
                _money(t["notional"], "R "), indent=1,
            )
        _kv("Total notional", _money(fx_tot_not, "R "))
        _kv("Total quantity (USD)", _money(fx_tot_qty, "$", 2))
        _formula("FX WA", f"{_money(fx_tot_not, 'R ')} / {_money(fx_tot_qty, '$', 2)}",
                 _money(fx_wa_d, "R ", 4), val_color=GOLD)
    else:
        _kv("No USD/ZAR trades found", "--")
    pdf.ln(4)

    # ── Step 4 — Implied Spot Rate ───────────────────────────────
    _step("4", "Implied Spot Rate Derivation",
          "ZAR cost per gram = (Gold WA x FX WA) / 31.1035")
    _formula("Spot ZAR/g",
             f"({_money(gold_wa_d, '$', 4)} x {_money(fx_wa_d, 'R ', 4)}) / 31.1035",
             _money(spot_d, "R ", 4), val_color=GOLD)
    pdf.ln(4)

    # ── Step 5 — USD Cash Flows ──────────────────────────────────
    _step("5", "StoneX USD Cash Flow (Sell Side)",
          "Sell = +inflow, Buy = -outflow. Net = total USD available.")
    for cf in xau_cfs:
        if cf["signed"] is not None:
            s = f"+{_money(cf['signed'], '$')}" if cf["signed"] >= 0 else f"-{_money(abs(cf['signed']), '$')}"
        else:
            s = "--"
        _kv(f"{cf['side']} {_oz(cf['qty'], 4)} @ {_money(cf['price'], '$', 4)}", s,
            val_color=GREEN if (cf["signed"] or 0) >= 0 else RED)
    _kv("Net USD cash flow", _money(net_usd, "$"),
        val_color=GREEN if net_usd >= 0 else RED)
    pdf.ln(4)

    # ── Step 6 — Control Account ─────────────────────────────────
    _step("6", "Control Account - Metal Exposure",
          "Zero = fully hedged. Residual = open exposure.")
    _kv("Unhedged (grams)", _grams(ctrl_g))
    _kv("Unhedged (troy oz)", _oz(ctrl_oz, 4))
    _kv("Unhedged (ZAR)", _money(ctrl_zar, "R "))
    pdf.ln(4)

    # ── Step 7 — ZAR Position ────────────────────────────────────
    _step("7", "ZAR Position - Sell vs Buy Side",
          "StoneX proceeds in ZAR vs TradeMC buy side (net of refining).")
    _kv("StoneX ZAR inflow", _money(stx_zar_flow, "R "))
    _kv("Sell side total (ZAR)", _money(sell_zar, "R "))
    _kv("Buy side total (ZAR)", _money(buy_zar, "R "))
    pdf.ln(4)

    # ── Step 8 — Profit & Loss ───────────────────────────────────
    _step("8", "Profit & Loss Determination",
          "Profit = sell side - buy side. Margin = profit / buy side x 100.")
    _formula("Profit (USD)",
             f"{_money(sell_usd, '$')} - {_money(buy_usd, '$')}",
             _money(profit_usd, "$"),
             val_color=GREEN if (_n(profit_usd) or 0) >= 0 else RED)
    _formula("Profit (ZAR)",
             f"{_money(sell_zar, 'R ')} - {_money(buy_zar, 'R ')}",
             _money(profit_zar, "R "),
             val_color=GREEN if (_n(profit_zar) or 0) >= 0 else RED)
    _formula("Profit margin",
             f"({_money(profit_zar, 'R ')} / {_money(buy_zar, 'R ')}) x 100",
             _pct(profit_pct, 3), val_color=GOLD)
    pdf.ln(3)

    # Profit highlight box
    if profit_zar is not None:
        _check_space(16)
        is_pos = profit_zar >= 0
        bg = GREEN_BG if is_pos else RED_BG
        bdr = GREEN if is_pos else RED
        clr = GREEN if is_pos else RED
        label = "NET PROFIT" if is_pos else "NET LOSS"

        y = pdf.get_y()
        h = 13
        pdf.set_fill_color(*bg)
        pdf.rect(LM, y, W, h, style="F")
        pdf.set_draw_color(*bdr)
        pdf.set_line_width(0.3)
        pdf.rect(LM, y, W, h, style="D")
        # Top accent
        pdf.set_fill_color(*bdr)
        pdf.rect(LM, y, W, 2, style="F")

        # Label
        pdf.set_xy(LM + 10, y + 4)
        pdf.set_text_color(*MID)
        pdf.set_font("Arial", "B", 8)
        pdf.cell(W * 0.25, 5, _t(f"{label} (ZAR)"), 0, 0, "L")

        # Value
        pdf.set_text_color(*clr)
        pdf.set_font("Arial", "B", 14)
        pdf.cell(W * 0.40, 5, _t(_money(profit_zar, "R ")), 0, 0, "R")

        # Margin
        pdf.set_font("Arial", "B", 9)
        pdf.cell(W * 0.25, 5, _t(f"({_pct(profit_pct, 3)} margin)"), 0, 0, "R")

        pdf.set_y(y + h + 5)

    # ── Confidentiality ──────────────────────────────────────────
    pdf.ln(6)
    pdf.set_draw_color(*BORDER)
    pdf.set_line_width(0.1)
    pdf.line(LM, pdf.get_y(), LM + W, pdf.get_y())
    pdf.ln(2)
    pdf.set_text_color(*FAINT)
    pdf.set_font("Arial", "", 5.5)
    pdf.multi_cell(W, 3, _t(
        "This document is confidential and intended solely for the use of "
        "Metal Concentrators SA and its authorised counterparties. "
        "Reproduction or distribution without prior written consent is prohibited."
    ), 0, "C")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  OUTPUT
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    out = pdf.output(dest="S")
    if isinstance(out, str):
        return out.encode("latin-1", errors="ignore")
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    return None
