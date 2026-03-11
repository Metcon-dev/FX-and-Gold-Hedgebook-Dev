"""Purchase order business logic"""
import pandas as pd


def load_purchase_orders(selected_month=None, suppress_warnings=False):
    """Load purchase orders from the combined Excel file"""
    try:
        # Load from combined PO file (single sheet)
        po_df = pd.read_excel('POs_Combined.xlsx', dtype=str, engine='openpyxl')
        po_df.columns = [str(col).strip() for col in po_df.columns]
        
        # Standardize column names
        column_mapping = {
            'PC Nr / Trade Nr': 'Trade_Number',
            'PC Nr': 'Trade_Number',
            'Trade Nr': 'Trade_Number',
            'Trade Requested Timestamp': 'Timestamp',
            'ZAR/gram': 'ZAR_per_gram',
            'ZAR/g': 'ZAR_per_gram',
            'Zar/gram including ref': 'ZAR_per_gram_incl_ref',
            'Confirmed Metal Price (USD/oz)': 'Metal_Price_USD_oz',
            'Metal Price (USD/oz)': 'Metal_Price_USD_oz',
            'Confirmed Exchange Rate (ZAR/USD)': 'Exchange_Rate_ZAR_USD',
            'Exchange Rate (ZAR/USD)': 'Exchange_Rate_ZAR_USD',
            'Debit (g)': 'Quantity_grams',
            'Debit': 'Quantity_grams',
            'Supplier': 'Supplier',
            'Month': 'Sheet_Name'
        }
        
        rename_dict = {old: new for old, new in column_mapping.items() if old in po_df.columns}
        if rename_dict:
            po_df = po_df.rename(columns=rename_dict)
        
        # Clean data
        po_df = po_df.dropna(how='all')
        
        if 'Trade_Number' in po_df.columns:
            po_df = po_df.dropna(subset=['Trade_Number'])
            po_df['Trade_Number'] = po_df['Trade_Number'].astype(str).str.strip()
        
        # Numeric conversion
        numeric_columns = ['ZAR_per_gram', 'ZAR_per_gram_incl_ref', 'Metal_Price_USD_oz', 
                          'Exchange_Rate_ZAR_USD', 'Quantity_grams']
        
        for col in numeric_columns:
            if col in po_df.columns:
                po_df[col] = po_df[col].astype(str).str.replace(r'[,R$ ]', '', regex=True)
                po_df[col] = pd.to_numeric(po_df[col], errors='coerce')
        
        if 'Quantity_grams' in po_df.columns:
            po_df = po_df.dropna(subset=['Quantity_grams'])
            po_df['Quantity_oz'] = po_df['Quantity_grams'] / 31.1035
        
        if 'Supplier' in po_df.columns:
            po_df = po_df.dropna(subset=['Supplier'])
            po_df['Supplier'] = po_df['Supplier'].astype(str).str.strip()
        
        # Parse timestamps
        if 'Timestamp' in po_df.columns:
            po_df['Timestamp'] = pd.to_datetime(po_df['Timestamp'], errors='coerce')
        
        # Filter by month if specified
        if selected_month and selected_month != 'All Months' and 'Sheet_Name' in po_df.columns:
            po_df = po_df[po_df['Sheet_Name'] == selected_month]
        
        return po_df
        
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def get_po_details_for_trade(trade_number):
    """Get detailed PO breakdown for a specific trade number.
    
    Returns a DataFrame with columns:
    - Supplier
    - Metal_Price_USD_oz (weighted average per supplier)
    - Exchange_Rate_ZAR_USD (weighted average per supplier)
    - Quantity_grams (total per supplier)
    - PO_Count (number of POs per supplier)
    """
    try:
        po_df = load_purchase_orders(suppress_warnings=True)
        
        if po_df.empty:
            return pd.DataFrame()
        
        # Filter for the specific trade number
        trade_number_str = str(trade_number).strip()
        if 'Trade_Number' not in po_df.columns:
            return pd.DataFrame()
        
        trade_pos = po_df[po_df['Trade_Number'] == trade_number_str].copy()
        
        if trade_pos.empty:
            return pd.DataFrame()
        
        # Ensure required columns exist
        if 'Supplier' not in trade_pos.columns or 'Quantity_grams' not in trade_pos.columns:
            return pd.DataFrame()
        
        # Group by supplier and calculate weighted averages
        result_rows = []
        for supplier, group in trade_pos.groupby('Supplier'):
            total_grams = group['Quantity_grams'].sum()
            po_count = len(group)
            
            # Weighted average metal price
            if 'Metal_Price_USD_oz' in group.columns and total_grams > 0:
                weighted_price = (group['Metal_Price_USD_oz'] * group['Quantity_grams']).sum() / total_grams
            else:
                weighted_price = 0
            
            # Weighted average exchange rate
            if 'Exchange_Rate_ZAR_USD' in group.columns and total_grams > 0:
                weighted_fx = (group['Exchange_Rate_ZAR_USD'] * group['Quantity_grams']).sum() / total_grams
            else:
                weighted_fx = 0
            
            result_rows.append({
                'Supplier': supplier,
                'Metal_Price_USD_oz': weighted_price,
                'Exchange_Rate_ZAR_USD': weighted_fx,
                'Quantity_grams': total_grams,
                'PO_Count': po_count
            })
        
        result_df = pd.DataFrame(result_rows)
        
        # Sort by quantity (largest first)
        if not result_df.empty:
            result_df = result_df.sort_values('Quantity_grams', ascending=False).reset_index(drop=True)
        
        return result_df
        
    except Exception:
        return pd.DataFrame()


def calculate_hedging_needs(trades_df, po_df):
    """Calculate hedging needs by comparing trades with purchase orders - ALL IN GRAMS"""
    
    if po_df.empty or trades_df.empty:
        return pd.DataFrame()
    
    # Filter only XAUUSD trades (gold trades)
    gold_trades = trades_df[trades_df['Symbol'] == 'XAUUSD'].copy()
    
    if gold_trades.empty:
        return pd.DataFrame()
    
    # Group trades by OrderID (which should match Trade_Number in PO sheet)
    trade_summary = []
    
    # Get unique trade numbers from both sources
    all_trade_numbers = set()
    if 'OrderID' in gold_trades.columns:
        valid_order_ids = [str(oid).strip() for oid in gold_trades['OrderID'].dropna().unique() if str(oid).strip()]
        all_trade_numbers.update(valid_order_ids)
    if 'Trade_Number' in po_df.columns:
        valid_po_numbers = [str(tn).strip() for tn in po_df['Trade_Number'].dropna().unique() if str(tn).strip()]
        all_trade_numbers.update(valid_po_numbers)
    
    TOLERANCE = 1.0  # 1 gram tolerance
    
    for trade_num in sorted(all_trade_numbers, key=lambda x: str(x)):
        po_for_trade = po_df[po_df['Trade_Number'] == str(trade_num)] if 'Trade_Number' in po_df.columns else pd.DataFrame()
        trades_for_trade = gold_trades[gold_trades['OrderID'] == str(trade_num)] if 'OrderID' in gold_trades.columns else pd.DataFrame()
        
        po_total_grams = po_for_trade['Quantity_grams'].sum() if not po_for_trade.empty and 'Quantity_grams' in po_for_trade.columns else 0
        
        net_sold_grams = 0
        if not trades_for_trade.empty:
            for _, trade in trades_for_trade.iterrows():
                trade_grams = trade['Quantity'] * 31.1035
                if trade['Side'] == 'SELL':
                    net_sold_grams += trade_grams
                else:
                    net_sold_grams -= trade_grams
        
        hedging_need_grams = po_total_grams - net_sold_grams
        
        if abs(hedging_need_grams) <= TOLERANCE:
            status = "✅ Fully Hedged"
        elif hedging_need_grams > TOLERANCE:
            status = f"⚠️ Under-Hedged ({hedging_need_grams:,.1f}g to hedge)"
        else:
            status = f"⚠️ Over-Hedged ({abs(hedging_need_grams):,.1f}g excess)"
        
        po_count = len(po_for_trade) if not po_for_trade.empty else 0
        
        trade_summary.append({
            'Trade Number': trade_num,
            'PO Quantity (g)': po_total_grams,
            'Net Sold (g)': net_sold_grams,
            'Hedging Need (g)': hedging_need_grams,
            'PO Count': po_count,
            'Status': status
        })
    
    return pd.DataFrame(trade_summary)
