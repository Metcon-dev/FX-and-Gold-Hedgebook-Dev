"""Trade business logic and calculations"""
import pandas as pd


def format_ledger_like_example(df):
    """
    Format ledger exactly like Excel example:
    Doc # | Trade Date | Value Date | Narration | Debit $+Au | Credit +$-Au | Balance $ | Debit Sell Zar | Credit Buy Zar | Balance Zar | Trade #

    NOTE: The Excel logic calculates **running USD and ZAR balances row-by-row** based on
    the Debit/Credit for each trade (not from the stored `balance_usd` / `balance_zar`
    columns in the database). This mirrors that behaviour so the **Balance Zar** column
    matches the spreadsheet. Balances reset to zero per Trade # (each trade is isolated),
    and debit/credit amounts are recomputed when missing so the columns populate like
    the Excel example.
    """
    # Work on a copy
    work_df = df.copy()

    # Sort to mirror spreadsheet ordering (by Trade Date then Doc # / id)
    sort_cols = [col for col in ['Trade Date', 'Doc #', 'id'] if col in work_df.columns]
    if sort_cols:
        work_df = work_df.sort_values(sort_cols)

    def normalize_trade_number(val):
        if pd.isna(val) or val == '':
            return ''
        s = str(val).strip()
        if s.endswith('.0') and s.replace('.', '', 1).isdigit():
            s = s[:-2]
        return s

    def safe_num(val):
        try:
            if val is None or val == '':
                return 0.0
            return float(val)
        except Exception:
            return 0.0

    # Normalize trade numbers and create per-row trade keys
    work_df['Trade #'] = work_df['OrderID'].apply(normalize_trade_number)
    work_df['DocClean'] = work_df['Doc #'].fillna('')
    work_df['TradeKey'] = work_df.apply(
        lambda r: r['Trade #'] if r['Trade #'] else f"__doc_{r['DocClean']}_{r.name}",
        axis=1
    )

    # Compute debit/credit values (recompute when zeros)
    def split_symbol(sym):
        sym = str(sym or '').upper()
        if '/' in sym:
            base, quote = sym.split('/', 1)
            return base, quote
        if len(sym) == 6:
            return sym[:3], sym[3:]
        return sym, ''

    def compute_amounts(row):
        symbol = row['Symbol']
        side = row['Side']
        qty = row['Quantity']
        price = row['Price']

        base, quote = split_symbol(symbol)

        debit_usd = safe_num(row.get('Debit USD', 0))
        credit_usd = safe_num(row.get('Credit USD', 0))
        debit_zar = safe_num(row.get('Debit ZAR', 0))
        credit_zar = safe_num(row.get('Credit ZAR', 0))

        usd_notional = qty * price if (base in {'XAU', 'XAG', 'XPT', 'XPD'} and quote == 'USD') else qty
        zar_notional = qty * price if (base == 'USD' and quote == 'ZAR') else 0.0

        if base in {'XAU', 'XAG', 'XPT', 'XPD'} and quote == 'USD':
            if side == 'SELL':
                if credit_usd == 0:
                    credit_usd = usd_notional
            else:  # BUY XAU
                if debit_usd == 0:
                    debit_usd = usd_notional
        elif base == 'USD' and quote == 'ZAR':
            if side == 'SELL':
                if debit_usd == 0:
                    debit_usd = usd_notional
                if credit_zar == 0:
                    credit_zar = zar_notional
            else:  # BUY USD
                if credit_usd == 0:
                    credit_usd = usd_notional
                if debit_zar == 0:
                    debit_zar = zar_notional

        return pd.Series({
            'DebitUSDCalc': debit_usd,
            'CreditUSDCalc': credit_usd,
            'DebitZARCalc': debit_zar,
            'CreditZARCalc': credit_zar
        })

    work_df = work_df.join(work_df.apply(compute_amounts, axis=1))

    # Per-trade running balances (start at zero for each trade)
    work_df['USDDiff'] = -work_df['DebitUSDCalc'] + work_df['CreditUSDCalc']
    work_df['ZARDiff'] = -work_df['DebitZARCalc'] + work_df['CreditZARCalc']
    work_df['BalanceUSD'] = work_df.groupby('TradeKey')['USDDiff'].cumsum()
    work_df['BalanceZAR'] = work_df.groupby('TradeKey')['ZARDiff'].cumsum()

    def format_number(val, decimals=2):
        if val == '' or val is None:
            return ''
        try:
            return f"{float(val):,.{decimals}f}"
        except Exception:
            return ''

    formatted_rows = []
    for _, trade in work_df.iterrows():
        doc_number = trade.get('Doc #', '') or ''
        if pd.isna(doc_number):
            doc_number = ''

        trade_date = trade.get('Trade Date', '')
        value_date = trade.get('Value Date', '')

        if pd.notna(trade_date):
            try:
                trade_date = pd.to_datetime(trade_date).strftime('%d-%b-%y')
            except Exception:
                trade_date = str(trade_date)
        else:
            trade_date = ''

        if pd.notna(value_date):
            try:
                value_date = pd.to_datetime(value_date).strftime('%d-%b-%y')
            except Exception:
                value_date = str(value_date)
        else:
            value_date = ''

        symbol = trade['Symbol']
        side = trade['Side']
        quantity = trade['Quantity']
        price = trade['Price']
        trade_number = trade['Trade #']

        base, quote = split_symbol(symbol)

        if base in {'XAU', 'XAG', 'XPT', 'XPD'} and quote == 'USD':
            pair = f"{base}/{quote}"
            if trade_number:
                narration = f"{trade_number} {pair} {quantity:.3f} OZ @ {price:.2f}"
            else:
                narration = f"{pair} {quantity:.3f} OZ @ {price:.2f}"
        elif base == 'USD' and quote == 'ZAR':
            narration = f"USD/ZAR {quantity:,.2f}  @ {price:.5f}"
        else:
            pair = f"{base}/{quote}" if base and quote else str(symbol)
            narration = f"{pair} {quantity:,.4f} @ {price:.5f}"

        debit_usd = format_number(trade['DebitUSDCalc']) if trade['DebitUSDCalc'] != 0 else ''
        credit_usd = format_number(trade['CreditUSDCalc']) if trade['CreditUSDCalc'] != 0 else ''
        debit_zar = format_number(trade['DebitZARCalc']) if trade['DebitZARCalc'] != 0 else ''
        credit_zar = format_number(trade['CreditZARCalc']) if trade['CreditZARCalc'] != 0 else ''

        trader_name = trade.get('Trader', '') or trade.get('trader_name', '')
        if pd.isna(trader_name):
            trader_name = ''
        else:
            trader_name = str(trader_name).strip() if trader_name else ''

        row_dict = {
            'Trade #': trade_number,
            'Doc #': doc_number,
            'Trade Date': trade_date,
            'Value Date': value_date,
            'Narration': narration,
            'Debit $+Au': debit_usd,
            'Credit +$-Au': credit_usd,
            'Balance $': format_number(trade['BalanceUSD']),
            'Debit Sell Zar': debit_zar,
            'Credit Buy Zar': credit_zar,
            'Balance Zar': format_number(trade['BalanceZAR']),
            'Trader': trader_name
        }

        formatted_rows.append(row_dict)

    return pd.DataFrame(formatted_rows)


def calculate_breakdown_excel_format(df):
    """Calculate breakdown in exact Excel format - ONE weighted average for each symbol"""

    usdzar_trades = df[df['Symbol'] == 'USDZAR'].copy()
    usdzar_breakdown = []

    if not usdzar_trades.empty:
        total_volume = 0
        total_settlement = 0

        for _, trade in usdzar_trades.iterrows():
            if trade['Side'] == 'SELL':
                volume = -trade['Debit USD'] if trade['Debit USD'] > 0 else -trade['Credit USD']
                settlement = trade['Credit ZAR'] if trade['Credit ZAR'] > 0 else -trade['Debit ZAR']
            else:
                volume = trade['Credit USD'] if trade['Credit USD'] > 0 else -trade['Debit USD']
                settlement = -trade['Debit ZAR'] if trade['Debit ZAR'] > 0 else trade['Credit ZAR']

            total_volume += volume
            total_settlement += settlement

            usdzar_breakdown.append({
                'Side': trade['Side'],
                'Volume': volume,
                'Price': trade['Price'],
                'Settlement': settlement,
                'Trade ID': trade.get('id', ''),
                'Doc #': trade.get('Doc #', '')
            })

        weighted_avg = abs(total_settlement) / abs(total_volume) if total_volume != 0 else 0

        usdzar_summary = {
            'total_volume': total_volume,
            'total_settlement': total_settlement,
            'weighted_avg': weighted_avg,
            'breakdown': usdzar_breakdown
        }
    else:
        usdzar_summary = None

    xauusd_trades = df[df['Symbol'] == 'XAUUSD'].copy()
    xauusd_breakdown = []

    if not xauusd_trades.empty:
        total_volume = 0
        total_settlement = 0

        for _, trade in xauusd_trades.iterrows():
            if trade['Side'] == 'SELL':
                volume = -trade['Debit XAU'] if trade['Debit XAU'] > 0 else -trade['Credit XAU']
                settlement = trade['Credit USD'] if trade['Credit USD'] > 0 else -trade['Debit USD']
            else:
                volume = trade['Credit XAU'] if trade['Credit XAU'] > 0 else -trade['Debit XAU']
                settlement = -trade['Debit USD'] if trade['Debit USD'] > 0 else trade['Credit USD']

            total_volume += volume
            total_settlement += settlement

            xauusd_breakdown.append({
                'Side': trade['Side'],
                'Volume': volume,
                'Price': trade['Price'],
                'Settlement': settlement,
                'Trade ID': trade.get('id', ''),
                'Doc #': trade.get('Doc #', '')
            })

        weighted_avg = abs(total_settlement) / abs(total_volume) if total_volume != 0 else 0

        xauusd_summary = {
            'total_volume': total_volume,
            'total_settlement': total_settlement,
            'weighted_avg': weighted_avg,
            'breakdown': xauusd_breakdown
        }
    else:
        xauusd_summary = None

    return {
        'USDZAR': usdzar_summary,
        'XAUUSD': xauusd_summary
    }


def create_breakdown_csv(breakdown_data, breakdown_df):
    """Create CSV data for trade breakdown"""
    from datetime import datetime
    csv_data = []

    csv_data.append(["Trade Breakdown Export", f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])
    csv_data.append([])

    if breakdown_data['USDZAR']:
        usdzar_data = breakdown_data['USDZAR']
        csv_data.append(["USD/ZAR TRADE BREAKDOWN"])
        csv_data.append(["Weighted Average Exchange Rate", f"R{usdzar_data['weighted_avg']:,.6f}"])
        csv_data.append([])
        csv_data.append(["Trade #", "Volume (USD)", "Rate", "Settlement (ZAR)"])
        csv_data.append(["TOTAL", f"({usdzar_data['total_volume']:,.2f})", f"{usdzar_data['weighted_avg']:,.8f}",
                         f"{usdzar_data['total_settlement']:,.2f}"])

        for trade in usdzar_data['breakdown']:
            csv_data.append([
                trade['Side'],
                f"({abs(trade['Volume']):,.2f})",
                f"{trade['Price']:,.4f}",
                f"{trade['Settlement']:,.2f}"
            ])

        csv_data.append([])
        csv_data.append(["Summary"])
        csv_data.append(["Total USD Volume", f"${usdzar_data['total_volume']:,.2f}"])
        csv_data.append(["Total ZAR Settlement", f"R{usdzar_data['total_settlement']:,.2f}"])
        csv_data.append(["Number of Trades", len(usdzar_data['breakdown'])])
        csv_data.append([])
        csv_data.append([])

    if breakdown_data['XAUUSD']:
        xauusd_data = breakdown_data['XAUUSD']
        csv_data.append(["XAU/USD TRADE BREAKDOWN"])
        csv_data.append(["Weighted Average Price", f"${xauusd_data['weighted_avg']:,.2f}"])
        csv_data.append([])
        csv_data.append(["Trade #", "Volume (XAU oz)", "Price", "Settlement (USD)"])
        csv_data.append(["TOTAL", f"({xauusd_data['total_volume']:,.4f})", f"{xauusd_data['weighted_avg']:,.4f}",
                         f"{xauusd_data['total_settlement']:,.2f}"])

        for trade in xauusd_data['breakdown']:
            csv_data.append([
                trade['Side'],
                f"({abs(trade['Volume']):,.4f})",
                f"{trade['Price']:,.2f}",
                f"{trade['Settlement']:,.2f}"
            ])

        csv_data.append([])
        csv_data.append(["Summary"])
        csv_data.append(["Total XAU Volume", f"{xauusd_data['total_volume']:,.4f} oz"])
        csv_data.append(["Total USD Settlement", f"${xauusd_data['total_settlement']:,.2f}"])
        csv_data.append(["Number of Trades", len(xauusd_data['breakdown'])])

    return csv_data
