def build_trading_ticket_pdf(trade_num_value: str,
                             trademc_rows: pd.DataFrame,
                             stonex_rows: pd.DataFrame,
                             summary_rows: pd.DataFrame):
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            HRFlowable, Image as RLImage, PageBreak, KeepTogether,
        )
        from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from io import BytesIO
        import html as _html
        import os as _os
    except Exception:
        return _build_trading_ticket_pdf_fpdf(
            trade_num_value,
            trademc_rows,
            stonex_rows,
            summary_rows,
        )

    def _esc(v: str) -> str:
        return _html.escape(str(v), quote=False)

    # ── Font registration (Calibri with Helvetica fallback) ──────
    FONT_REG  = "Helvetica"
    FONT_BOLD = "Helvetica-Bold"
    FONT_IT   = "Helvetica-Oblique"
    try:
        _rp = r"C:\Windows\Fonts\calibri.ttf"
        _bp = r"C:\Windows\Fonts\calibrib.ttf"
        _ip = r"C:\Windows\Fonts\calibrii.ttf"
        if _os.path.isfile(_rp) and _os.path.isfile(_bp):
            pdfmetrics.registerFont(TTFont("Calibri",      _rp))
            pdfmetrics.registerFont(TTFont("Calibri-Bold", _bp))
            FONT_REG  = "Calibri"
            FONT_BOLD = "Calibri-Bold"
            if _os.path.isfile(_ip):
                pdfmetrics.registerFont(TTFont("Calibri-Italic", _ip))
                FONT_IT = "Calibri-Italic"
    except Exception:
        pass

    # ── Brand palette ────────────────────────────────────────────
    CHARCOAL     = colors.HexColor("#1C1C1C")
    COPPER       = colors.HexColor("#B07840")
    COPPER_LT    = colors.HexColor("#FBF5EC")
    COPPER_MD    = colors.HexColor("#E8D5B7")
    COPPER_DK    = colors.HexColor("#8A5C2E")
    NAVY         = colors.HexColor("#1E2A38")
    CREAM_TXT    = colors.HexColor("#F5EDE3")
    ROW_ALT      = colors.HexColor("#FAFAFA")
    BORDER       = colors.HexColor("#E2E2E2")
    MID_GREY     = colors.HexColor("#888888")
    DARK_GREY    = colors.HexColor("#555555")
    LIGHT_GREY   = colors.HexColor("#F4F4F4")
    GREEN        = colors.HexColor("#1A7A42")
    GREEN_LT     = colors.HexColor("#E8F5EE")
    RED_C        = colors.HexColor("#C0392B")
    RED_LT       = colors.HexColor("#FDECEB")
    WHITE        = colors.white

    # ── Numeric / text helpers ───────────────────────────────────
    def _s(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v).strip()

    def _n(v) -> Optional[float]:
        try:
            f = float(v)
            return None if pd.isna(f) else f
        except Exception:
            return None

    def _f(v, dp=2, prefix="") -> str:
        n = _n(v)
        return "\u2014" if n is None else f"{prefix}{n:,.{dp}f}"

    def _money(v, prefix="$", dp=2) -> str:
        n = _n(v)
        return "\u2014" if n is None else f"{prefix}{n:,.{dp}f}"

    def _grams(v) -> str:
        n = _n(v)
        return "\u2014" if n is None else f"{n:,.2f} g"

    def _ounces(v, dp=4) -> str:
        n = _n(v)
        return "\u2014" if n is None else f"{n:,.{dp}f} oz"

    def _pct(v, dp=2) -> str:
        n = _n(v)
        return "\u2014" if n is None else f"{n:,.{dp}f}%"

    def _tr(t: str, mx: int) -> str:
        return t if len(t) <= mx else t[:mx - 1] + "\u2026"

    # ── Document setup ───────────────────────────────────────────
    buf  = BytesIO()
    PAGE = landscape(A4)
    LM = RM = 20 * mm
    TM = 18 * mm
    BM = 16 * mm
    doc  = SimpleDocTemplate(buf, pagesize=PAGE,
                             leftMargin=LM, rightMargin=RM,
                             topMargin=TM, bottomMargin=BM)
    PW = PAGE[0] - LM - RM

    # ── Style factory ────────────────────────────────────────────
    _style_count = [0]
    def _ps(name, **kw) -> ParagraphStyle:
        _style_count[0] += 1
        uname = f"{name}_{_style_count[0]}"
        return ParagraphStyle(
            uname,
            fontName=kw.pop("fontName", FONT_REG),
            fontSize=kw.pop("fontSize", 8),
            leading=kw.pop("leading", 11),
            textColor=kw.pop("textColor", CHARCOAL),
            **kw,
        )

    generated_at = datetime.now().strftime("%d %B %Y at %H:%M")
    story = []

    # ── Logo ─────────────────────────────────────────────────────
    _logo_path = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
        "MetCon Logo.png",
    )
    _logo_cell: object = Spacer(60 * mm, 16 * mm)
    try:
        if _os.path.isfile(_logo_path):
            from PIL import Image as _PILImage
            with _PILImage.open(_logo_path) as _pimg:
                _pimg.load()
            _logo_cell = RLImage(_logo_path, width=60 * mm, height=16 * mm)
    except Exception:
        pass

    # ════════════════════════════════════════════════════════════
    # HEADER
    # ════════════════════════════════════════════════════════════
    title_para = Paragraph(
        f"<font name='{FONT_BOLD}' size='8' color='#B07840'>METAL CONCENTRATORS SA</font><br/>"
        f"<font name='{FONT_BOLD}' size='22' color='#1C1C1C'>Trade Breakdown Report</font><br/>"
        f"<font name='{FONT_BOLD}' size='12' color='#B07840'>Trade #{_esc(_s(trade_num_value))}</font>",
        _ps("_hdr", leading=26, alignment=TA_RIGHT),
    )
    date_para = Paragraph(
        f"<font size='7.5' color='#888888'>Report generated on {_esc(generated_at)}</font>",
        _ps("_hdr_date", fontSize=7.5, textColor=MID_GREY, alignment=TA_RIGHT),
    )
    hdr = Table([[_logo_cell, title_para]], colWidths=[PW * 0.38, PW * 0.62])
    hdr.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(hdr)
    story.append(date_para)
    story.append(Spacer(1, 3 * mm))
    story.append(HRFlowable(width=PW, thickness=3, color=COPPER,
                            spaceBefore=0, spaceAfter=0.8 * mm))
    story.append(HRFlowable(width=PW, thickness=0.75, color=COPPER_MD,
                            spaceBefore=0, spaceAfter=6 * mm))

    # ════════════════════════════════════════════════════════════
    # REUSABLE LAYOUT HELPERS
    # ════════════════════════════════════════════════════════════
    def _sec_lbl(title: str, description: str = ""):
        """Section header with copper left bar, title, and optional description."""
        inner_rows = [[Paragraph(
            f"<font name='{FONT_BOLD}' size='10' color='#1C1C1C'>{_esc(title.upper())}</font>",
            _ps("_sl", fontName=FONT_BOLD, fontSize=10, textColor=CHARCOAL, leading=14),
        )]]
        if description:
            inner_rows.append([Paragraph(
                f"<font size='7.5' color='#777777'>{_esc(description)}</font>",
                _ps("_sld", fontSize=7.5, textColor=MID_GREY, leading=10),
            )])
        inner = Table(inner_rows, colWidths=[PW - 8])
        inner.setStyle(TableStyle([
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ("TOPPADDING",    (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ]))
        lbl = Table([[Spacer(1, 1), inner]], colWidths=[5, PW - 5])
        lbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, 0), COPPER),
            ("BACKGROUND",    (1, 0), (1, 0), COPPER_LT),
            ("LEFTPADDING",   (0, 0), (0, 0), 0),
            ("RIGHTPADDING",  (0, 0), (0, 0), 0),
            ("LEFTPADDING",   (1, 0), (-1, -1), 12),
            ("RIGHTPADDING",  (1, 0), (-1, -1), 12),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(lbl)
        story.append(Spacer(1, 4 * mm))

    def _audit_step_header(step_num: str, title: str, subtitle: str = ""):
        """Numbered audit step with prominent badge and explanatory text."""
        badge = Paragraph(
            f"<font name='{FONT_BOLD}' size='10' color='#FFFFFF'>\u00a0{_esc(step_num)}\u00a0</font>",
            _ps("_badge", fontName=FONT_BOLD, fontSize=10, textColor=WHITE, alignment=TA_CENTER),
        )
        title_p = Paragraph(
            f"<font name='{FONT_BOLD}' size='11' color='#1C1C1C'>{_esc(title)}</font>",
            _ps("_ash", fontName=FONT_BOLD, fontSize=11, leading=15),
        )
        row_tbl = Table([[badge, title_p]], colWidths=[26 * mm, PW - 26 * mm])
        row_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, 0), COPPER),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING",   (0, 0), (0, 0), 6),
            ("RIGHTPADDING",  (0, 0), (0, 0), 6),
            ("LEFTPADDING",   (1, 0), (1, 0), 10),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(row_tbl)
        if subtitle:
            story.append(Spacer(1, 2 * mm))
            story.append(Paragraph(
                f"<font name='{FONT_IT}' size='7.5' color='#666666'>{_esc(subtitle)}</font>",
                _ps("_ass", fontName=FONT_IT, fontSize=7.5, textColor=DARK_GREY, leftIndent=8, rightIndent=8),
            ))
        story.append(Spacer(1, 3 * mm))
        story.append(HRFlowable(width=PW, thickness=0.5, color=COPPER_MD,
                                spaceBefore=0, spaceAfter=3.5 * mm))

    def _audit_row(label: str, value: str, bold_value: bool = True,
                   value_color=None, indent: int = 0):
        """Key-value audit row with separator."""
        vc = value_color or CHARCOAL
        lpad = 12 + indent * 16
        lbl_cell = Paragraph(
            f"<font size='8' color='#444444'>{_esc(label)}</font>",
            _ps("_arl", fontSize=8, textColor=DARK_GREY, leading=11),
        )
        val_font = FONT_BOLD if bold_value else FONT_REG
        val_cell = Paragraph(
            f"<font name='{val_font}' size='8.5'>{_esc(value)}</font>",
            _ps("_arv", fontName=val_font, fontSize=8.5, textColor=vc,
                alignment=TA_RIGHT, leading=12),
        )
        tbl = Table([[lbl_cell, val_cell]], colWidths=[PW * 0.55, PW * 0.45])
        tbl.setStyle(TableStyle([
            ("LEFTPADDING",   (0, 0), (0, 0), lpad),
            ("RIGHTPADDING",  (-1, 0), (-1, 0), 12),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LINEBELOW",     (0, 0), (-1, -1), 0.25, BORDER),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(tbl)

    def _audit_row_formula(label: str, formula: str, result: str,
                           value_color=None, indent: int = 0):
        """Three-column audit row: label | formula -> result."""
        vc = value_color or CHARCOAL
        lpad = 12 + indent * 16
        lbl_cell = Paragraph(
            f"<font size='7.5' color='#444444'>{_esc(label)}</font>",
            _ps("_arf_l", fontSize=7.5, textColor=DARK_GREY, leading=10),
        )
        formula_cell = Paragraph(
            f"<font name='{FONT_IT}' size='7' color='#999999'>{_esc(formula)}  \u2192</font>",
            _ps("_arf_f", fontName=FONT_IT, fontSize=7, textColor=MID_GREY, alignment=TA_RIGHT, leading=10),
        )
        val_cell = Paragraph(
            f"<font name='{FONT_BOLD}' size='8.5'>{_esc(result)}</font>",
            _ps("_arf_v", fontName=FONT_BOLD, fontSize=8.5, textColor=vc,
                alignment=TA_RIGHT, leading=12),
        )
        tbl = Table([[lbl_cell, formula_cell, val_cell]],
                     colWidths=[PW * 0.28, PW * 0.42, PW * 0.30])
        tbl.setStyle(TableStyle([
            ("LEFTPADDING",   (0, 0), (0, 0), lpad),
            ("RIGHTPADDING",  (-1, 0), (-1, 0), 12),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LINEBELOW",     (0, 0), (-1, -1), 0.25, BORDER),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(tbl)

    def _audit_sub_title(title: str):
        """Sub-section title with copper left accent."""
        tbl = Table([[Paragraph(
            f"<font name='{FONT_BOLD}' size='8' color='#2A2A2A'>{_esc(title)}</font>",
            _ps("_ast", fontName=FONT_BOLD, fontSize=8, textColor=CHARCOAL),
        )]], colWidths=[PW])
        tbl.setStyle(TableStyle([
            ("LEFTPADDING",   (0, 0), (-1, -1), 12),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("BACKGROUND",    (0, 0), (-1, -1), LIGHT_GREY),
            ("LINEBEFORE",    (0, 0), (0, 0), 2.5, COPPER),
            ("LINEBELOW",     (0, 0), (-1, -1), 0.3, BORDER),
        ]))
        story.append(tbl)

    _pos_st = _ps("_pos", fontName=FONT_BOLD, fontSize=8.5, textColor=GREEN,  alignment=TA_RIGHT)
    _neg_st = _ps("_neg", fontName=FONT_BOLD, fontSize=8.5, textColor=RED_C,  alignment=TA_RIGHT)

    def _data_tbl(col_names, col_pcts, rows, right_cols=None, profit_col=None, totals=None):
        """Professional striped data table with navy header and copper accents."""
        right_cols = right_cols or []
        if not rows:
            story.append(Paragraph(
                "<font color='#999999'>\u00a0\u00a0No data available for this section.</font>",
                _ps("_nd", textColor=MID_GREY, fontSize=8),
            ))
            story.append(Spacer(1, 5 * mm))
            return

        cw = [PW * p for p in col_pcts]

        hdr_row = [
            Paragraph(
                _esc(col),
                _ps(f"_th", fontName=FONT_BOLD, fontSize=7.5, leading=10,
                    textColor=CREAM_TXT, alignment=TA_RIGHT if ci in right_cols else TA_CENTER),
            )
            for ci, col in enumerate(col_names)
        ]
        tdata = [hdr_row]

        for row in rows:
            cells = []
            for ci, col in enumerate(col_names):
                raw = row.get(col, "")
                txt = _esc(_s(raw)) if raw != "" else "\u2014"
                if col == profit_col:
                    nv = _n(raw)
                    cells.append(Paragraph(
                        txt if txt != "\u2014" else "",
                        _pos_st if (nv is not None and nv >= 0) else _neg_st,
                    ))
                elif ci in right_cols:
                    cells.append(Paragraph(txt, _ps(f"_tdr", fontSize=8, alignment=TA_RIGHT)))
                else:
                    cells.append(Paragraph(_esc(_tr(_s(raw), 42)), _ps(f"_td", fontSize=8)))
            tdata.append(cells)

        if totals:
            tot_cells = []
            for ci, col in enumerate(col_names):
                raw = totals.get(col, "")
                txt = _esc(_s(raw)) if raw != "" else ""
                tot_cells.append(Paragraph(txt, _ps(
                    f"_tot", fontName=FONT_BOLD, fontSize=8.5,
                    alignment=TA_RIGHT if ci in right_cols else TA_LEFT,
                )))
            tdata.append(tot_cells)

        tbl = Table(tdata, colWidths=cw, repeatRows=1)
        ts = TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), NAVY),
            ("LINEBELOW",     (0, 0), (-1, 0), 2.5, COPPER),
            ("TOPPADDING",    (0, 0), (-1, 0), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
            ("TOPPADDING",    (0, 1), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("LINEBELOW",     (0, 1), (-1, -1), 0.3, BORDER),
        ])
        for ri in range(1, len(tdata)):
            if totals and ri == len(tdata) - 1:
                ts.add("BACKGROUND", (0, ri), (-1, ri), COPPER_LT)
                ts.add("LINEABOVE",  (0, ri), (-1, ri), 1.5, COPPER)
                ts.add("LINEBELOW",  (0, ri), (-1, ri), 1.5, COPPER)
            else:
                ts.add("BACKGROUND", (0, ri), (-1, ri), ROW_ALT if ri % 2 == 0 else WHITE)
        tbl.setStyle(ts)
        story.append(tbl)
        story.append(Spacer(1, 5 * mm))

    # ════════════════════════════════════════════════════════════
    # PRE-COMPUTE ALL AUDIT TRAIL DATA
    # ════════════════════════════════════════════════════════════
    tm_pdf = trademc_rows.copy() if trademc_rows is not None else pd.DataFrame()
    stx_pdf = stonex_rows.copy() if stonex_rows is not None else pd.DataFrame()
    sum_pdf = summary_rows.copy() if summary_rows is not None else pd.DataFrame()

    # -- TradeMC bookings with refining --
    bookings = []
    tm_total_g = tm_total_oz = tm_total_usd = tm_total_zar_gross = tm_total_zar_net = 0.0
    if not tm_pdf.empty:
        for _, r in tm_pdf.iterrows():
            weight_g = _n(r.get("Weight (g)")) or 0.0
            weight_oz = _n(r.get("Weight (oz)")) or (weight_g / 31.1035)
            booked_price = _n(r.get("$/oz Booked"))
            fx_rate = _n(r.get("FX Rate"))
            usd_val_raw = _n(r.get("USD Value"))
            usd_val = usd_val_raw if usd_val_raw is not None else (
                weight_oz * booked_price if booked_price is not None else None)
            zar_gross_raw = _n(r.get("ZAR Value"))
            zar_gross = zar_gross_raw if zar_gross_raw is not None else (
                usd_val * fx_rate if usd_val is not None and fx_rate is not None else None)
            refining_rate = _n(r.get("company_refining_rate")) or 0.0
            refining_deduction = zar_gross * (refining_rate / 100.0) if zar_gross is not None else None
            zar_net_raw = _n(r.get("zar_value_less_refining"))
            zar_net = zar_net_raw if zar_net_raw is not None else (
                zar_gross * (1.0 - refining_rate / 100.0) if zar_gross is not None else None)
            company = _s(r.get("Company")) or _s(r.get("company_name")) or "Unknown"
            bookings.append({
                "company": company, "weight_g": weight_g, "weight_oz": weight_oz,
                "booked_price": booked_price, "fx_rate": fx_rate,
                "usd_value": usd_val, "zar_gross": zar_gross,
                "refining_rate": refining_rate, "refining_deduction": refining_deduction,
                "zar_net": zar_net,
            })
            tm_total_g += weight_g
            tm_total_oz += weight_oz
            tm_total_usd += usd_val or 0.0
            tm_total_zar_gross += zar_gross or 0.0
            tm_total_zar_net += zar_net or 0.0

    # -- StoneX trade classification --
    xau_trades = []
    fx_trades = []
    all_pmx = []
    if not stx_pdf.empty:
        for _, r in stx_pdf.iterrows():
            sym = str(r.get("Symbol", "")).upper().replace("/", "").replace("-", "").replace(" ", "")
            side = str(r.get("Side", "")).upper().strip()
            qty = abs(_n(r.get("Quantity")) or 0.0)
            price = _n(r.get("Price"))
            notional = qty * price if price is not None and qty > 0 else None
            trade_info = {"symbol": sym, "side": side, "qty": qty, "price": price,
                          "notional": notional,
                          "trade_date": _s(r.get("Trade Date")),
                          "value_date": _s(r.get("Value Date")),
                          "fnc": _s(r.get("FNC #")), "narration": _s(r.get("Narration"))}
            if qty > 0:
                all_pmx.append(trade_info)
                if sym == "XAUUSD":
                    xau_trades.append(trade_info)
                elif sym == "USDZAR":
                    fx_trades.append(trade_info)

    # -- Weighted averages --
    gold_wa_total_notional = sum(t["notional"] or 0 for t in xau_trades)
    gold_wa_total_qty = sum(t["qty"] for t in xau_trades)
    gold_wa = gold_wa_total_notional / gold_wa_total_qty if gold_wa_total_qty > 1e-9 else None

    fx_wa_total_notional = sum(t["notional"] or 0 for t in fx_trades)
    fx_wa_total_qty = sum(t["qty"] for t in fx_trades)
    fx_wa = fx_wa_total_notional / fx_wa_total_qty if fx_wa_total_qty > 1e-9 else None

    # -- Spot ZAR/g --
    spot_zar_per_g = (gold_wa * fx_wa) / 31.1035 if gold_wa and fx_wa else None

    # -- Cash flows --
    xau_cash_flows = []
    net_stonex_usd = 0.0
    for t in xau_trades:
        signed = None
        if t["notional"] is not None:
            signed = t["notional"] if t["side"] == "SELL" else -t["notional"]
        xau_cash_flows.append({**t, "signed": signed})
        net_stonex_usd += signed or 0.0

    # -- Summary data --
    base = sum_pdf.iloc[0].to_dict() if not sum_pdf.empty else {}
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
    total_traded_g = _n(base.get("Total Traded (g)"))
    total_traded_oz = _n(base.get("Total Traded (oz)"))
    stonex_zar_flow = _n(base.get("StoneX ZAR Flow"))
    sum_gold_wa = _n(base.get("Gold WA $/oz"))
    sum_fx_wa = _n(base.get("FX WA USD/ZAR"))
    sum_spot_zar_g = _n(base.get("Spot ZAR/g"))

    gold_wa_display = sum_gold_wa if sum_gold_wa is not None else gold_wa
    fx_wa_display = sum_fx_wa if sum_fx_wa is not None else fx_wa
    spot_display = sum_spot_zar_g if sum_spot_zar_g is not None else spot_zar_per_g

    if profit_zar is None and sell_zar is not None and buy_zar is not None:
        profit_zar = sell_zar - buy_zar
    if profit_usd is None and sell_usd is not None and buy_usd is not None:
        profit_usd = sell_usd - buy_usd
    if profit_pct is None and profit_zar is not None and buy_zar and abs(buy_zar) > 1e-12:
        profit_pct = (profit_zar / buy_zar) * 100.0
    if ctrl_oz is None and ctrl_g is not None:
        ctrl_oz = ctrl_g / 31.1035
    if total_traded_oz is None and total_traded_g is not None:
        total_traded_oz = total_traded_g / 31.1035

    stonex_g = total_traded_g or (total_traded_oz * 31.1035 if total_traded_oz else None)

    # ════════════════════════════════════════════════════════════
    # SECTION 1 - EXECUTIVE SUMMARY (KPI dashboard)
    # ════════════════════════════════════════════════════════════
    _sec_lbl("Executive Summary",
             "Key performance indicators for this trade at a glance.")

    # -- Primary KPI cards --
    def _make_kpi_card(label: str, value: str, bg_color=WHITE,
                       value_color=CHARCOAL, border_color=BORDER,
                       accent_color=COPPER):
        lbl_p = Paragraph(
            f"<font name='{FONT_BOLD}' size='6.5' color='#777777'>{_esc(label.upper())}</font>",
            _ps("_kl", fontName=FONT_BOLD, fontSize=6.5, textColor=MID_GREY,
                alignment=TA_CENTER, leading=9),
        )
        val_p = Paragraph(
            f"<font name='{FONT_BOLD}' size='13'>{_esc(value)}</font>",
            _ps("_kv", fontName=FONT_BOLD, fontSize=13, leading=17,
                textColor=value_color, alignment=TA_CENTER),
        )
        card = Table([[lbl_p], [val_p]], colWidths=[PW / 4 - 5 * mm])
        card.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), bg_color),
            ("BOX",           (0, 0), (-1, -1), 0.5, border_color),
            ("LINEABOVE",     (0, 0), (-1, 0), 3, accent_color),
            ("TOPPADDING",    (0, 0), (-1, 0),  8),
            ("BOTTOMPADDING", (0, -1), (-1, -1), 10),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return card

    profit_color = GREEN if (profit_zar is not None and profit_zar >= 0) else RED_C
    profit_bg = GREEN_LT if (profit_zar is not None and profit_zar >= 0) else RED_LT

    kpi_row = [
        _make_kpi_card("Total Weight Traded",
                       _grams(stonex_g)),
        _make_kpi_card("Gold WA Price",
                       _money(gold_wa_display, "$", 3) + "/oz" if gold_wa_display else "\u2014"),
        _make_kpi_card("Weighted Avg FX Rate",
                       f"R\u00a0{fx_wa_display:,.4f}" if fx_wa_display else "\u2014"),
        _make_kpi_card("Net Profit (ZAR)",
                       _money(profit_zar, "R\u00a0"),
                       bg_color=profit_bg, value_color=profit_color,
                       border_color=profit_color, accent_color=profit_color),
    ]
    kpi_table = Table([kpi_row], colWidths=[PW / 4] * 4)
    kpi_table.setStyle(TableStyle([
        ("LEFTPADDING",   (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 2),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 3 * mm))

    # -- Secondary metrics row --
    kpi_row2_items = []
    if spot_display is not None:
        kpi_row2_items.append(("Implied Spot ZAR/g", f"R\u00a0{spot_display:,.4f}"))
    if stonex_zar_flow is not None:
        kpi_row2_items.append(("StoneX ZAR Inflow", _money(stonex_zar_flow, "R\u00a0")))
    if sell_usd is not None:
        kpi_row2_items.append(("Sell Side (USD)", _money(sell_usd, "$")))
    if buy_usd is not None:
        kpi_row2_items.append(("Buy Side (USD)", _money(buy_usd, "$")))
    if profit_pct is not None:
        kpi_row2_items.append(("Profit Margin", _pct(profit_pct, 3)))
    if ctrl_g is not None:
        kpi_row2_items.append(("Control Account", _grams(ctrl_g)))

    if kpi_row2_items:
        n_items = len(kpi_row2_items)
        cw_each = PW / max(n_items, 1)
        lbl_cells = []
        val_cells = []
        for lbl, val in kpi_row2_items:
            lbl_cells.append(Paragraph(
                f"<font name='{FONT_BOLD}' size='6' color='#999999'>{_esc(lbl.upper())}</font>",
                _ps("_k2l", fontName=FONT_BOLD, fontSize=6, textColor=MID_GREY,
                    alignment=TA_CENTER, leading=8),
            ))
            val_cells.append(Paragraph(
                f"<font name='{FONT_BOLD}' size='9.5'>{_esc(val)}</font>",
                _ps("_k2v", fontName=FONT_BOLD, fontSize=9.5, leading=13,
                    textColor=CHARCOAL, alignment=TA_CENTER),
            ))
        k2_tbl = Table([lbl_cells, val_cells], colWidths=[cw_each] * n_items)
        k2_tbl.setStyle(TableStyle([
            ("TOPPADDING",    (0, 0), (-1, 0), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 3),
            ("TOPPADDING",    (0, 1), (-1, 1), 3),
            ("BOTTOMPADDING", (0, 1), (-1, 1), 7),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("BOX",           (0, 0), (-1, -1), 0.3, BORDER),
            ("LINEBELOW",     (0, 0), (-1, 0),  0.3, BORDER),
            ("LINEBEFORE",    (1, 0), (-1, -1), 0.3, BORDER),
            ("BACKGROUND",    (0, 0), (-1, -1), LIGHT_GREY),
        ]))
        story.append(k2_tbl)
    story.append(Spacer(1, 7 * mm))

    # ════════════════════════════════════════════════════════════
    # SECTION 2 - CLIENT BOOKINGS (Buy Side)
    # ════════════════════════════════════════════════════════════
    _sec_lbl("Client Bookings \u2014 Buy Side",
             "Gold purchased from TradeMC clients, valued in USD and converted to ZAR with refining deductions applied.")

    TM_COLS = ["Company", "Weight (g)", "Weight (oz)", "$/oz Booked", "FX Rate",
               "USD Value", "ZAR Gross", "Refining %", "ZAR Net"]
    _TM_W = {"Company": 0.19, "Weight (g)": 0.09, "Weight (oz)": 0.09,
             "$/oz Booked": 0.10, "FX Rate": 0.09, "USD Value": 0.11,
             "ZAR Gross": 0.11, "Refining %": 0.09, "ZAR Net": 0.13}

    tm_table_rows = []
    for b in bookings:
        tm_table_rows.append({
            "Company": b["company"],
            "Weight (g)": _f(b["weight_g"], 2),
            "Weight (oz)": _f(b["weight_oz"], 4),
            "$/oz Booked": _f(b["booked_price"], 2),
            "FX Rate": _f(b["fx_rate"], 4),
            "USD Value": _f(b["usd_value"], 2),
            "ZAR Gross": _f(b["zar_gross"], 2),
            "Refining %": _pct(b["refining_rate"]),
            "ZAR Net": _f(b["zar_net"], 2),
        })
    tm_right = list(range(1, len(TM_COLS)))
    tm_wids = [_TM_W[c] for c in TM_COLS]

    tm_totals_row = {
        "Company": "TOTAL",
        "Weight (g)": f"{tm_total_g:,.2f}",
        "Weight (oz)": f"{tm_total_oz:,.4f}",
        "USD Value": f"{tm_total_usd:,.2f}",
        "ZAR Gross": f"{tm_total_zar_gross:,.2f}",
        "ZAR Net": f"{tm_total_zar_net:,.2f}",
    } if bookings else None

    _data_tbl(TM_COLS, tm_wids, tm_table_rows,
              right_cols=tm_right, totals=tm_totals_row)

    # ════════════════════════════════════════════════════════════
    # SECTION 3 - PMX / STONEX TRADES (Sell Side)
    # ════════════════════════════════════════════════════════════
    _sec_lbl("StoneX / PMX Trades \u2014 Sell Side",
             "Gold sold and FX hedged through StoneX. These trades generate the USD and ZAR proceeds used to settle client bookings.")

    STX_COLS = ["FNC #", "Trade Date", "Value Date", "Symbol", "Side", "Narration", "Quantity", "Price"]
    stx_avail = [c for c in STX_COLS if c in stx_pdf.columns]
    _STX_W = {"FNC #": 0.13, "Trade Date": 0.09, "Value Date": 0.09, "Symbol": 0.07,
              "Side": 0.06, "Narration": 0.24, "Quantity": 0.14, "Price": 0.12}
    _sw = sum(_STX_W.get(c, 0.10) for c in stx_avail) or 1
    stx_wids = [_STX_W.get(c, 0.10) / _sw for c in stx_avail]
    stx_right = [i for i, c in enumerate(stx_avail) if c in ("Quantity", "Price")]

    stx_rows_data: list = []
    for _, r in stx_pdf.iterrows():
        rd: Dict[str, str] = {}
        for c in stx_avail:
            if c == "Quantity":   rd[c] = _f(r.get(c), 2)
            elif c == "Price":    rd[c] = _f(r.get(c), 4)
            else:                 rd[c] = _s(r.get(c))
        stx_rows_data.append(rd)

    _data_tbl(stx_avail, stx_wids, stx_rows_data, right_cols=stx_right)

    # ════════════════════════════════════════════════════════════
    # PAGE BREAK - DETAILED AUDIT TRAIL
    # ════════════════════════════════════════════════════════════
    story.append(PageBreak())

    # Audit trail header
    audit_hdr = Paragraph(
        f"<font name='{FONT_BOLD}' size='18' color='#1C1C1C'>Detailed Calculation Audit Trail</font>",
        _ps("_audit_hdr", fontName=FONT_BOLD, fontSize=18, leading=22),
    )
    audit_sub = Paragraph(
        f"<font name='{FONT_BOLD}' size='9' color='#B07840'>Trade #{_esc(_s(trade_num_value))}</font>"
        f"<font size='8' color='#777777'>  \u2014  Step-by-step breakdown of every calculation, "
        f"designed for reconciliation, compliance review, and independent verification.</font>",
        _ps("_audit_sub", fontSize=9, leading=13),
    )
    story.append(audit_hdr)
    story.append(Spacer(1, 2 * mm))
    story.append(audit_sub)
    story.append(Spacer(1, 3 * mm))
    story.append(HRFlowable(width=PW, thickness=3, color=COPPER,
                            spaceBefore=0, spaceAfter=0.8 * mm))
    story.append(HRFlowable(width=PW, thickness=0.75, color=COPPER_MD,
                            spaceBefore=0, spaceAfter=6 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 1 - Source Data Overview
    # ──────────────────────────────────────────────────────────
    _audit_step_header("1", "Source Data Overview",
                       "A summary of the raw data loaded from TradeMC (buy side) and "
                       "StoneX/PMX (sell side) for this trading ticket. These counts confirm "
                       "all expected trades have been captured before calculations begin.")
    _audit_row("TradeMC client bookings (buy side)", str(len(bookings)))
    _audit_row("StoneX/PMX execution trades (sell side)", str(len(all_pmx)))
    _audit_row("  \u2022  XAU/USD gold price trades", str(len(xau_trades)), indent=1)
    _audit_row("  \u2022  USD/ZAR foreign exchange trades", str(len(fx_trades)), indent=1)
    _audit_row("Troy ounce conversion factor", "1 troy oz = 31.1035 grams")
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 2 - Client Booking Valuation
    # ──────────────────────────────────────────────────────────
    _audit_step_header("2", "Client Booking Valuation (Buy Side)",
                       "Each TradeMC booking is converted from grams to troy ounces, "
                       "priced in USD at the booked gold rate, converted to ZAR at the "
                       "client\u2019s FX rate, and then reduced by the applicable refining charge. "
                       "The net ZAR value represents the total cost of acquiring gold from this client.")

    for idx, b in enumerate(bookings):
        _audit_sub_title(f"Booking {idx + 1}: {b['company']}")
        _audit_row("Raw weight received", _grams(b["weight_g"]), indent=1)
        _audit_row_formula("Converted to troy ounces",
                           f"{_f(b['weight_g'], 2)} g \u00f7 31.1035",
                           _ounces(b["weight_oz"], 6), indent=1)
        _audit_row("Client\u2019s booked gold price", _money(b["booked_price"], "$") + " per troy oz", indent=1)
        _audit_row_formula("USD value of booking",
                           f"{_ounces(b['weight_oz'], 6)} \u00d7 {_money(b['booked_price'], '$')}",
                           _money(b["usd_value"], "$"), indent=1)
        _audit_row("FX rate applied (ZAR per USD)", _f(b["fx_rate"], 4), indent=1)
        _audit_row_formula("ZAR gross value (before refining)",
                           f"{_money(b['usd_value'], '$')} \u00d7 {_f(b['fx_rate'], 4)}",
                           _money(b["zar_gross"], "R\u00a0"), indent=1)
        _audit_row("Refining charge rate", _pct(b["refining_rate"]), indent=1)
        _audit_row_formula("Refining deduction amount",
                           f"{_money(b['zar_gross'], 'R ')} \u00d7 {_pct(b['refining_rate'])}",
                           _money(b["refining_deduction"], "R\u00a0"), indent=1)
        _audit_row("ZAR net value (after refining)", _money(b["zar_net"], "R\u00a0"),
                   value_color=COPPER_DK, indent=1)

    _audit_sub_title("Aggregated Buy Side Totals")
    _audit_row("Combined weight", f"{_grams(tm_total_g)} ({_ounces(tm_total_oz, 6)})")
    _audit_row("Combined USD value", _money(tm_total_usd, "$"))
    _audit_row("Combined ZAR gross (before refining)", _money(tm_total_zar_gross, "R\u00a0"))
    _audit_row("Combined ZAR net (after refining)", _money(tm_total_zar_net, "R\u00a0"),
               value_color=COPPER_DK)
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 3 - Weighted Average Pricing
    # ──────────────────────────────────────────────────────────
    _audit_step_header("3", "Weighted Average Price Calculation",
                       "The weighted average is computed as the sum of all trade notional "
                       "values divided by the sum of all trade quantities. This ensures that "
                       "larger trades carry proportionally more weight in the average, "
                       "giving an accurate blended execution rate.")

    _audit_sub_title("Gold Weighted Average (XAU/USD \u2014 price per troy ounce)")
    if xau_trades:
        for idx, t in enumerate(xau_trades):
            _audit_row_formula(
                f"Trade {idx+1}: {t['side']} {_ounces(t['qty'], 4)} @ {_money(t['price'], '$', 4)}",
                f"{_ounces(t['qty'], 4)} \u00d7 {_money(t['price'], '$', 4)}",
                _money(t["notional"], "$"),
                indent=1,
            )
        _audit_row("Total notional value (sum of qty \u00d7 price)", _money(gold_wa_total_notional, "$"))
        _audit_row("Total quantity traded", _ounces(gold_wa_total_qty, 4))
        _audit_row_formula("Resulting Gold Weighted Average",
                           f"{_money(gold_wa_total_notional, '$')} \u00f7 {_ounces(gold_wa_total_qty, 4)}",
                           _money(gold_wa_display, "$", 4),
                           value_color=COPPER)
    else:
        _audit_row("No XAU/USD trades found", "\u2014")

    story.append(Spacer(1, 3 * mm))

    _audit_sub_title("FX Weighted Average (USD/ZAR \u2014 rand per dollar)")
    if fx_trades:
        for idx, t in enumerate(fx_trades):
            _audit_row_formula(
                f"Trade {idx+1}: {t['side']} {_money(t['qty'], '$', 2)} @ {_money(t['price'], 'R\u00a0', 4)}",
                f"{_money(t['qty'], '$', 2)} \u00d7 {_money(t['price'], 'R\u00a0', 4)}",
                _money(t["notional"], "R\u00a0"),
                indent=1,
            )
        _audit_row("Total notional value (sum of qty \u00d7 rate)", _money(fx_wa_total_notional, "R\u00a0"))
        _audit_row("Total quantity traded (USD)", _money(fx_wa_total_qty, "$", 2))
        _audit_row_formula("Resulting FX Weighted Average",
                           f"{_money(fx_wa_total_notional, 'R ')} \u00f7 {_money(fx_wa_total_qty, '$', 2)}",
                           _money(fx_wa_display, "R\u00a0", 4),
                           value_color=COPPER)
    else:
        _audit_row("No USD/ZAR trades found", "\u2014")
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 4 - Implied Spot Rate
    # ──────────────────────────────────────────────────────────
    _audit_step_header("4", "Implied Spot Rate Derivation",
                       "Derives the effective ZAR cost per gram of gold by combining "
                       "the gold weighted average ($/oz) with the FX weighted average (ZAR/$) "
                       "and dividing by the troy ounce conversion factor. This rate represents "
                       "the blended cost of acquiring gold through StoneX.")
    _audit_row_formula("Implied spot price per gram (ZAR)",
                       f"({_money(gold_wa_display, '$', 4)} \u00d7 {_money(fx_wa_display, 'R ', 4)}) \u00f7 31.1035",
                       _money(spot_display, "R\u00a0", 4),
                       value_color=COPPER)
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 5 - USD Cash Flows
    # ──────────────────────────────────────────────────────────
    _audit_step_header("5", "StoneX USD Cash Flow Analysis (Sell Side)",
                       "Tracks the net USD proceeds from StoneX gold trades. "
                       "Sell transactions generate positive cash inflows (+), while "
                       "buy transactions represent negative outflows (\u2212). The net "
                       "figure represents the total USD available from execution.")
    for cf in xau_cash_flows:
        sign_str = ""
        if cf["signed"] is not None:
            sign_str = f"+{_money(cf['signed'], '$')}" if cf["signed"] >= 0 else f"\u2212{_money(abs(cf['signed']), '$')}"
        else:
            sign_str = "\u2014"
        _audit_row(
            f"{cf['side']} {_ounces(cf['qty'], 4)} @ {_money(cf['price'], '$', 4)}",
            sign_str,
            value_color=GREEN if (cf["signed"] or 0) >= 0 else RED_C,
        )
    _audit_row("Net USD cash flow from StoneX", _money(net_stonex_usd, "$"),
               value_color=GREEN if net_stonex_usd >= 0 else RED_C)
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 6 - Metal Exposure (Control Account)
    # ──────────────────────────────────────────────────────────
    _audit_step_header("6", "Control Account \u2014 Metal Exposure",
                       "The control account tracks remaining unhedged metal. "
                       "A zero balance indicates that all gold purchased from clients "
                       "has been fully sold on StoneX. Any residual balance represents "
                       "open exposure that has not yet been hedged.")
    _audit_row("Unhedged position (grams)", _grams(ctrl_g))
    _audit_row("Unhedged position (troy ounces)", _ounces(ctrl_oz, 4))
    _audit_row("Unhedged position valued in ZAR", _money(ctrl_zar, "R\u00a0"))
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 7 - ZAR Position Summary
    # ──────────────────────────────────────────────────────────
    _audit_step_header("7", "ZAR Position \u2014 Sell vs Buy Side Comparison",
                       "StoneX USD proceeds are converted to ZAR at the traded FX rate "
                       "to establish the sell side valuation. This is compared against "
                       "the TradeMC buy side (net of refining) to determine the trade\u2019s profitability.")
    _audit_row("StoneX ZAR inflow (from FX conversion)", _money(stonex_zar_flow, "R\u00a0"))
    _audit_row("Sell side total (ZAR received)", _money(sell_zar, "R\u00a0"))
    _audit_row("Buy side total (ZAR paid to clients, net of refining)", _money(buy_zar, "R\u00a0"))
    story.append(Spacer(1, 5 * mm))

    # ──────────────────────────────────────────────────────────
    # STEP 8 - Final Profit / Loss
    # ──────────────────────────────────────────────────────────
    _audit_step_header("8", "Profit & Loss Determination",
                       "The final profit is calculated as the difference between "
                       "what was received from StoneX (sell side) and what was paid "
                       "to TradeMC clients (buy side). Profit margin is expressed as "
                       "a percentage of the buy side cost.")

    _audit_row_formula("Profit in USD",
                       f"{_money(sell_usd, '$')} \u2212 {_money(buy_usd, '$')}",
                       _money(profit_usd, "$"),
                       value_color=GREEN if (profit_usd or 0) >= 0 else RED_C)
    _audit_row_formula("Profit in ZAR",
                       f"{_money(sell_zar, 'R ')} \u2212 {_money(buy_zar, 'R ')}",
                       _money(profit_zar, "R\u00a0"),
                       value_color=GREEN if (profit_zar or 0) >= 0 else RED_C)
    _audit_row_formula("Profit margin (% of buy side cost)",
                       f"({_money(profit_zar, 'R ')} \u00f7 {_money(buy_zar, 'R ')}) \u00d7 100",
                       _pct(profit_pct, 3),
                       value_color=COPPER)
    story.append(Spacer(1, 4 * mm))

    # Profit highlight box
    if profit_zar is not None:
        p_bg = GREEN_LT if profit_zar >= 0 else RED_LT
        p_border = GREEN if profit_zar >= 0 else RED_C
        p_color = GREEN if profit_zar >= 0 else RED_C
        p_label = "NET PROFIT" if profit_zar >= 0 else "NET LOSS"
        profit_box_data = [[
            Paragraph(
                f"<font name='{FONT_BOLD}' size='10' color='#555555'>{p_label} (ZAR)</font>",
                _ps("_pb_l", fontName=FONT_BOLD, fontSize=10, textColor=DARK_GREY,
                    leading=13),
            ),
            Paragraph(
                f"<font name='{FONT_BOLD}' size='18'>{_esc(_money(profit_zar, 'R '))}</font>",
                _ps("_pb_v", fontName=FONT_BOLD, fontSize=18, leading=22,
                    textColor=p_color, alignment=TA_RIGHT),
            ),
            Paragraph(
                f"<font name='{FONT_BOLD}' size='11'>({_esc(_pct(profit_pct, 3))} margin)</font>",
                _ps("_pb_p", fontName=FONT_BOLD, fontSize=11,
                    textColor=p_color, alignment=TA_RIGHT, leading=14),
            ),
        ]]
        pbox = Table(profit_box_data, colWidths=[PW * 0.25, PW * 0.45, PW * 0.30])
        pbox.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), p_bg),
            ("BOX",           (0, 0), (-1, -1), 1.5, p_border),
            ("LINEABOVE",     (0, 0), (-1, 0), 3.5, p_border),
            ("LEFTPADDING",   (0, 0), (-1, -1), 18),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 18),
            ("TOPPADDING",    (0, 0), (-1, -1), 14),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(pbox)
    story.append(Spacer(1, 10 * mm))

    # ── Footer on every page ─────────────────────────────────
    def _footer_on_page(canvas_obj, doc_obj):
        canvas_obj.saveState()
        canvas_obj.setStrokeColor(COPPER_MD)
        canvas_obj.setLineWidth(0.5)
        canvas_obj.line(LM, 13 * mm, PAGE[0] - RM, 13 * mm)
        canvas_obj.setFont(FONT_REG, 7)
        canvas_obj.setFillColor(MID_GREY)
        footer_left = (
            f"Metal Concentrators SA  \u2022  Confidential  \u2022  "
            f"Trade Breakdown #{_s(trade_num_value)}"
        )
        footer_right = f"Generated {generated_at}  \u2022  Page {canvas_obj.getPageNumber()}"
        canvas_obj.drawString(LM, 8.5 * mm, footer_left)
        canvas_obj.drawRightString(PAGE[0] - RM, 8.5 * mm, footer_right)
        canvas_obj.restoreState()

    doc.build(story, onFirstPage=_footer_on_page, onLaterPages=_footer_on_page)
    return buf.getvalue()
