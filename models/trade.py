"""Trade data models and database operations"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
import json
import os
import re
from models.database import get_db_connection

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MANUAL_TRADES_FILE = os.path.join(PROJECT_ROOT, "manual_trades.json")
SUPPORT_DOC_PATTERN = re.compile(r"(FNC|SWT|FCT)/[^\s,;]+", re.IGNORECASE)


def _normalize_trade_key(value) -> str:
    """Normalize trade/doc identifiers (e.g., convert 44999435.0 -> 44999435)."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    try:
        n = float(s)
        if n.is_integer():
            return str(int(n))
    except Exception:
        pass
    return s


def _extract_supporting_doc(*values) -> str:
    """
    Extract a supporting document token like FNC/... or SWT/... from candidate values.
    """
    for value in values:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        text = str(value).strip()
        if not text:
            continue
        match = SUPPORT_DOC_PATTERN.search(text)
        if match:
            return match.group(0).strip()
    return ""


def load_all_trades():
    """Load all trades from database - handles missing FIX columns"""
    conn = get_db_connection()
    try:
        # First check which columns exist
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(trades)")
        columns_info = cursor.fetchall()
        existing_columns = [col[1] for col in columns_info]
        
        # Build query based on available columns
        base_columns = [
            'id',
            'trade_date as "Trade Date"',
            'value_date as "Value Date"',
            'symbol as "Symbol"',
            'side as "Side"',
            'narration as "Narration"',
            'quantity as "Quantity"',
            'price as "Price"',
            'settle_currency as "Settle Currency"',
            'settle_amount as "Settle Amount"',
            'doc_number as "Doc #"',
            'clord_id as "ClOrdID"',
            'order_id as "OrderID"',
            'fnc_number as "FNC #"',
            'debit_usd as "Debit USD"',
            'credit_usd as "Credit USD"',
            'debit_zar as "Debit ZAR"',
            'credit_zar as "Credit ZAR"',
            'debit_xau as "Debit XAU"',
            'credit_xau as "Credit XAU"',
            'debit_xag as "Debit XAG"',
            'credit_xag as "Credit XAG"',
            'debit_xpt as "Debit XPT"',
            'credit_xpt as "Credit XPT"',
            'debit_xpd as "Debit XPD"',
            'credit_xpd as "Credit XPD"',
            'balance_usd as "Balance USD"',
            'balance_zar as "Balance ZAR"',
            'balance_xau as "Balance XAU"',
            'balance_xag as "Balance XAG"',
            'balance_xpt as "Balance XPT"',
            'balance_xpd as "Balance XPD"',
            'source_system as "Source System"',
            'created_at as "Created At"'
        ]
        
        # Add FIX columns if they exist
        fix_columns = [
            'fix_trade_id as "FIX Trade ID"',
            'fix_clord_id as "FIX ClOrdID"',
            'fix_exec_id as "FIX Exec ID"',
            'fix_account as "FIX Account"',
            'trader_name as "Trader"'
        ]
        
        # Always include FIX Trade ID for FNC number display
        if 'fix_trade_id' not in existing_columns:
            # Column doesn't exist yet, will be added by initialize_database
            pass
        
        # Check which FIX columns exist and add them to the query
        for col in fix_columns:
            col_name = col.split(' as ')[0]
            if col_name in existing_columns:
                base_columns.append(col)
        
        # Build the final query
        query = f"""
        SELECT {', '.join(base_columns)}
        FROM trades
        ORDER BY doc_number ASC
        """
        
        df = pd.read_sql(query, conn)
        
        # Convert date columns
        date_columns = ['Trade Date', 'Value Date', 'Created At']
        for col in date_columns:
            if col in df.columns and not df.empty:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        raise Exception(f"Error loading data: {e}")
    finally:
        conn.close()


def get_latest_stonex_trade_date(source_system: Optional[str] = "Murex") -> Optional[str]:
    """
    Get the latest StoneX trade date stored in the local trades table.

    Returns:
        str in YYYYMMDD format, or None when no date is available.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if source_system:
            cursor.execute(
                """
                SELECT MAX(trade_date)
                FROM trades
                WHERE source_system = ?
                  AND trade_date IS NOT NULL
                  AND TRIM(trade_date) != ''
                """,
                (source_system,),
            )
        else:
            cursor.execute(
                """
                SELECT MAX(trade_date)
                FROM trades
                WHERE trade_date IS NOT NULL
                  AND TRIM(trade_date) != ''
                """
            )

        latest = cursor.fetchone()
        latest_val = latest[0] if latest else None
        if not latest_val:
            return None

        dt = pd.to_datetime(latest_val, errors="coerce")
        if pd.isna(dt):
            digits = "".join(ch for ch in str(latest_val) if ch.isdigit())
            return digits[:8] if len(digits) >= 8 else None
        return dt.strftime("%Y%m%d")
    except Exception:
        return None
    finally:
        conn.close()


def get_latest_stonex_trade_id(source_system: Optional[str] = "Murex") -> Optional[int]:
    """
    Get the latest numeric StoneX trade/document ID stored in the local trades table.

    Returns:
        int or None when no numeric doc_number is available.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        base_query = """
            SELECT MAX(CAST(TRIM(doc_number) AS INTEGER))
            FROM trades
            WHERE doc_number IS NOT NULL
              AND TRIM(doc_number) != ''
              AND TRIM(doc_number) NOT GLOB '*[^0-9]*'
        """
        if source_system:
            cursor.execute(base_query + " AND source_system = ?", (source_system,))
        else:
            cursor.execute(base_query)
        row = cursor.fetchone()
        latest = row[0] if row else None
        return int(latest) if latest is not None else None
    except Exception:
        return None
    finally:
        conn.close()


def get_stonex_trade_date_bounds(source_system: Optional[str] = "Murex") -> Dict[str, Optional[str]]:
    """
    Get min/max StoneX trade dates stored locally.

    Returns:
        {
            "min_date": "YYYYMMDD" | None,
            "max_date": "YYYYMMDD" | None,
        }
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if source_system:
            cursor.execute(
                """
                SELECT MIN(trade_date), MAX(trade_date)
                FROM trades
                WHERE source_system = ?
                  AND trade_date IS NOT NULL
                  AND TRIM(trade_date) != ''
                """,
                (source_system,),
            )
        else:
            cursor.execute(
                """
                SELECT MIN(trade_date), MAX(trade_date)
                FROM trades
                WHERE trade_date IS NOT NULL
                  AND TRIM(trade_date) != ''
                """
            )

        row = cursor.fetchone() or (None, None)

        def _to_yyyymmdd(value) -> Optional[str]:
            if value is None:
                return None
            dt = pd.to_datetime(value, errors="coerce")
            if pd.notna(dt):
                return dt.strftime("%Y%m%d")
            digits = "".join(ch for ch in str(value) if ch.isdigit())
            return digits[:8] if len(digits) >= 8 else None

        return {"min_date": _to_yyyymmdd(row[0]), "max_date": _to_yyyymmdd(row[1])}
    except Exception:
        return {"min_date": None, "max_date": None}
    finally:
        conn.close()


def count_missing_support_docs(source_system: Optional[str] = "Murex") -> Dict[str, int]:
    """Count trades with/without supporting doc values (FNC/SWT token)."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if source_system:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE source_system = ?
                """,
                (source_system,),
            )
            total = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE source_system = ?
                  AND (fnc_number IS NULL OR TRIM(fnc_number) = '')
                """,
                (source_system,),
            )
        else:
            cursor.execute("SELECT COUNT(*) FROM trades")
            total = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE fnc_number IS NULL OR TRIM(fnc_number) = ''
                """
            )

        missing = int(cursor.fetchone()[0] or 0)
        return {
            "total": total,
            "missing": missing,
            "with_support_doc": max(total - missing, 0),
        }
    except Exception:
        return {"total": 0, "missing": 0, "with_support_doc": 0}
    finally:
        conn.close()


def backfill_support_docs_from_history_df(
    history_df: pd.DataFrame,
    source_system: Optional[str] = "Murex",
    only_missing: bool = True,
) -> Dict[str, int]:
    """
    Backfill supporting docs (FNC/SWT) for already-saved trades using StoneX history rows.

    Matching order:
    1. Exact doc/trade id match (doc_number/rest_trade_id vs TradeId/Id/RecId)
    2. Strict trade signature (date/value/symbol/side/qty/price)
    """
    if history_df is None or history_df.empty:
        return {
            "history_rows": 0,
            "history_with_support_doc": 0,
            "candidate_rows": 0,
            "updated": 0,
            "matched_by_doc_number": 0,
            "matched_by_signature": 0,
            "unresolved_after": 0,
        }

    def first_non_empty(*vals):
        for v in vals:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s and s.lower() != "nan":
                return s
        return ""

    def normalize_date(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        text = str(value).strip()
        if not text:
            return ""
        dt = pd.to_datetime(text, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) == 8:
            return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
        return ""

    def normalize_symbol(currency_pair, symbol) -> str:
        pair = first_non_empty(currency_pair).upper()
        if pair:
            return pair.replace("/", "")
        return first_non_empty(symbol).replace("/", "").upper()

    def normalize_side(value) -> str:
        side = first_non_empty(value).upper()
        return side

    def normalize_qty(row) -> float:
        qty = pd.to_numeric(row.get("LastQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(row.get("BaseQuantity"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(row.get("BuyQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(row.get("SellQty"), errors="coerce")
        if pd.isna(qty):
            return 0.0
        return abs(float(qty))

    def normalize_price(row) -> float:
        px = pd.to_numeric(row.get("LastPx"), errors="coerce")
        if pd.isna(px):
            px = pd.to_numeric(row.get("Price"), errors="coerce")
        if pd.isna(px):
            return 0.0
        return float(px)

    def make_signature(
        trade_date: str,
        value_date: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
    ) -> Optional[Tuple]:
        if not trade_date or not symbol or not side:
            return None
        return (
            trade_date,
            value_date or "",
            symbol,
            side,
            round(float(qty), 6),
            round(float(price), 6),
        )

    doc_map: Dict[str, str] = {}
    sig_map: Dict[Tuple, str] = {}
    ambiguous_doc_keys = set()
    ambiguous_sig_keys = set()
    history_with_support_doc = 0

    for _, r in history_df.iterrows():
        support_doc = _extract_supporting_doc(
            r.get("NeoId"),
            r.get("TagNumber"),
            r.get("OrderId"),
            r.get("ClOrdId"),
            r.get("Comment"),
            r.get("Comments"),
            r.get("ContractDescription"),
            r.get("Narration"),
            r.get("Description"),
        )
        if not support_doc:
            continue

        history_with_support_doc += 1

        history_key = _normalize_trade_key(
            first_non_empty(r.get("TradeId"), r.get("Id"), r.get("RecId"))
        )
        if history_key:
            prev = doc_map.get(history_key)
            if prev and prev != support_doc:
                ambiguous_doc_keys.add(history_key)
            else:
                doc_map[history_key] = support_doc

        signature = make_signature(
            normalize_date(first_non_empty(r.get("TradeDate"))),
            normalize_date(first_non_empty(r.get("ValueDate"))),
            normalize_symbol(r.get("CurrencyPair"), r.get("Symbol")),
            normalize_side(r.get("Side")),
            normalize_qty(r),
            normalize_price(r),
        )
        if signature:
            prev_sig = sig_map.get(signature)
            if prev_sig and prev_sig != support_doc:
                ambiguous_sig_keys.add(signature)
            else:
                sig_map[signature] = support_doc

    for k in ambiguous_doc_keys:
        doc_map.pop(k, None)
    for k in ambiguous_sig_keys:
        sig_map.pop(k, None)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        where_clauses = ["1=1"]
        params: List = []
        if source_system:
            where_clauses.append("source_system = ?")
            params.append(source_system)
        if only_missing:
            where_clauses.append("(fnc_number IS NULL OR TRIM(fnc_number) = '')")

        query = f"""
            SELECT id, doc_number, rest_trade_id, trade_date, value_date, symbol, side, quantity, price
            FROM trades
            WHERE {' AND '.join(where_clauses)}
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()

        updates_by_id: Dict[int, str] = {}
        matched_by_doc = 0
        matched_by_sig = 0

        for (
            trade_id,
            raw_doc_number,
            raw_rest_trade_id,
            raw_trade_date,
            raw_value_date,
            raw_symbol,
            raw_side,
            raw_qty,
            raw_price,
        ) in rows:
            support_doc = ""

            doc_keys = [
                _normalize_trade_key(raw_doc_number),
                _normalize_trade_key(raw_rest_trade_id),
            ]
            for key in doc_keys:
                if key and key in doc_map:
                    support_doc = doc_map[key]
                    matched_by_doc += 1
                    break

            if not support_doc:
                signature = make_signature(
                    normalize_date(raw_trade_date),
                    normalize_date(raw_value_date),
                    normalize_symbol("", raw_symbol),
                    normalize_side(raw_side),
                    float(raw_qty or 0.0),
                    float(raw_price or 0.0),
                )
                if signature and signature in sig_map:
                    support_doc = sig_map[signature]
                    matched_by_sig += 1

            if support_doc:
                updates_by_id[int(trade_id)] = support_doc

        updates = [(doc, trade_id) for trade_id, doc in updates_by_id.items()]
        if updates:
            cursor.executemany(
                """
                UPDATE trades
                SET fnc_number = ?
                WHERE id = ?
                """,
                updates,
            )

        # Also backfill from order_id when manually keyed as FNC/SWT.
        cursor.execute(
            """
            UPDATE trades
            SET fnc_number = order_id
            WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
              AND (
                    order_id LIKE 'FNC/%'
                 OR order_id LIKE 'SWT/%'
                 OR order_id LIKE 'FCT/%'
              )
            """
        )

        missing_where = ["(fnc_number IS NULL OR TRIM(fnc_number) = '')"]
        missing_params: List = []
        if source_system:
            missing_where.append("source_system = ?")
            missing_params.append(source_system)
        cursor.execute(
            f"SELECT COUNT(*) FROM trades WHERE {' AND '.join(missing_where)}",
            missing_params,
        )
        unresolved_after = int(cursor.fetchone()[0] or 0)

        conn.commit()
        return {
            "history_rows": int(len(history_df)),
            "history_with_support_doc": int(history_with_support_doc),
            "candidate_rows": int(len(rows)),
            "updated": int(len(updates)),
            "matched_by_doc_number": int(matched_by_doc),
            "matched_by_signature": int(matched_by_sig),
            "unresolved_after": int(unresolved_after),
            "doc_map_size": int(len(doc_map)),
            "signature_map_size": int(len(sig_map)),
        }
    finally:
        conn.close()


def add_fix_trade(trade_data: Dict) -> bool:
    """Add a trade from FIX API to database with balance calculation"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Set debit/credit based on symbol and side
        symbol = trade_data['symbol']
        side = trade_data['side']
        quantity = trade_data['quantity']
        price = trade_data['price']
        
        # Reset all to zero first
        for key in ['debit_usd', 'credit_usd', 'debit_zar', 'credit_zar', 'debit_xau', 'credit_xau']:
            trade_data[key] = 0
        
        if symbol == "XAUUSD":
            if side == "SELL":
                # SELL XAU (receive USD, give XAU)
                trade_data['debit_xau'] = quantity  # XAU goes out
                trade_data['credit_usd'] = quantity * price  # USD comes in
            else:  # BUY
                # BUY XAU (pay USD, receive XAU)
                trade_data['credit_xau'] = quantity  # XAU comes in
                trade_data['debit_usd'] = quantity * price  # USD goes out
                
        elif symbol == "USDZAR":
            if side == "SELL":
                # SELL USD (pay USD, receive ZAR)
                trade_data['debit_usd'] = quantity  # USD goes out
                trade_data['credit_zar'] = quantity * price  # ZAR comes in
            else:  # BUY
                # BUY USD (receive USD, pay ZAR)
                trade_data['credit_usd'] = quantity  # USD comes in
                trade_data['debit_zar'] = quantity * price  # ZAR goes out
        
        # Get latest balances
        cursor.execute("""
            SELECT balance_usd, balance_zar, balance_xau 
            FROM trades 
            ORDER BY trade_date DESC, created_at DESC 
            LIMIT 1
        """)
        latest_balances = cursor.fetchone()
        
        if latest_balances:
            balance_usd = latest_balances[0] + (trade_data['credit_usd'] - trade_data['debit_usd'])
            balance_zar = latest_balances[1] + (trade_data['credit_zar'] - trade_data['debit_zar'])
            balance_xau = latest_balances[2] + (trade_data['credit_xau'] - trade_data['debit_xau'])
        else:
            # First trade
            balance_usd = trade_data['credit_usd'] - trade_data['debit_usd']
            balance_zar = trade_data['credit_zar'] - trade_data['debit_zar']
            balance_xau = trade_data['credit_xau'] - trade_data['debit_xau']
        
        # Insert into database
        cursor.execute('''
        INSERT INTO trades (
            trade_date, value_date, symbol, side, narration, quantity, price,
            settle_currency, settle_amount, doc_number, clord_id, order_id,
            debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
            balance_usd, balance_zar, balance_xau,
            fix_message, fix_trade_id, fix_clord_id, fix_exec_id, fix_account,
            fix_settlement_date, fix_transact_time, fix_report_type, trader_name, fix_trans_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade_data['trade_date'], trade_data['value_date'], trade_data['symbol'],
            trade_data['side'], trade_data['narration'], trade_data['quantity'],
            trade_data['price'], trade_data['settle_currency'], trade_data['settle_amount'],
            trade_data['doc_number'], trade_data['clord_id'], trade_data['order_id'],
            trade_data['debit_usd'], trade_data['credit_usd'],
            trade_data['debit_zar'], trade_data['credit_zar'],
            trade_data['debit_xau'], trade_data['credit_xau'],
            balance_usd, balance_zar, balance_xau,
            trade_data['fix_message'], trade_data['fix_trade_id'], trade_data['fix_clord_id'],
            trade_data['fix_exec_id'], trade_data['fix_account'],
            trade_data['fix_settlement_date'], trade_data['fix_transact_time'],
            trade_data['fix_report_type'], trade_data.get('trader_name', ''), trade_data.get('fix_trans_type', '0')
        ))
        
        conn.commit()
        return True
        
    except sqlite3.IntegrityError as e:
        conn.rollback()
        # Handle duplicate doc_number error
        if "UNIQUE constraint failed: trades.doc_number" in str(e):
            # Generate a new unique doc_number
            import time
            timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
            trade_data['doc_number'] = f"FIX-{timestamp_str}-{int(time.time()*1000) % 1000}"
            # Retry with new doc_number
            return add_fix_trade(trade_data)
        else:
            raise Exception(f"Database integrity error: {e}")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error adding FIX trade: {e}")
    finally:
        conn.close()


def add_new_trade(trade_data):
    """Add a new trade manually (from sidebar form)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT balance_usd, balance_zar, balance_xau 
            FROM trades 
            ORDER BY trade_date DESC, created_at DESC 
            LIMIT 1
        """)
        latest_balances = cursor.fetchone()

        if latest_balances:
            balance_usd = latest_balances[0] + (trade_data['credit_usd'] - trade_data['debit_usd'])
            balance_zar = latest_balances[1] + (trade_data['credit_zar'] - trade_data['debit_zar'])
            balance_xau = latest_balances[2] + (trade_data['credit_xau'] - trade_data['debit_xau'])
        else:
            balance_usd = trade_data['credit_usd'] - trade_data['debit_usd']
            balance_zar = trade_data['credit_zar'] - trade_data['debit_zar']
            balance_xau = trade_data['credit_xau'] - trade_data['debit_xau']

        cursor.execute('''
        INSERT INTO trades (
            trade_date, value_date, symbol, side, narration, quantity, price,
            settle_currency, settle_amount, doc_number, clord_id, order_id,
            debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
            balance_usd, balance_zar, balance_xau
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade_data['trade_date'], trade_data['value_date'], trade_data['symbol'],
            trade_data['side'], trade_data['narration'], trade_data['quantity'],
            trade_data['price'], trade_data['settle_currency'], trade_data['settle_amount'],
            trade_data['doc_number'], trade_data.get('clord_id'), trade_data['order_id'],
            trade_data['debit_usd'], trade_data['credit_usd'],
            trade_data['debit_zar'], trade_data['credit_zar'],
            trade_data['debit_xau'], trade_data['credit_xau'],
            balance_usd, balance_zar, balance_xau
        ))

        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        conn.rollback()
        if "UNIQUE constraint failed: trades.doc_number" in str(e):
            raise Exception(f"Document number '{trade_data['doc_number']}' already exists. Please use a unique document number.")
        else:
            raise Exception(f"Database integrity error: {e}")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error adding trade: {e}")
    finally:
        conn.close()


def update_trade_order_id(trade_id: int, order_id: str) -> bool:
    """Update the order_id (MetCon Trade Number) for a specific trade"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Convert empty string to None for database
        order_id_value = order_id.strip().upper() if order_id and order_id.strip() else None
        
        cursor.execute('''
        UPDATE trades 
        SET order_id = ?
        WHERE id = ?
        ''', (order_id_value, trade_id))
        if cursor.rowcount == 0:
            cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
            if cursor.fetchone() is None:
                conn.rollback()
                return False

        # If a single supporting doc exists for this Trade #, propagate it to blank rows.
        if order_id_value:
            cursor.execute(
                """
                SELECT DISTINCT fnc_number
                FROM trades
                WHERE order_id = ?
                  AND fnc_number IS NOT NULL
                  AND TRIM(fnc_number) != ''
                """,
                (order_id_value,),
            )
            docs = [row[0] for row in cursor.fetchall() if row and row[0]]
            if len(docs) == 1:
                cursor.execute(
                    """
                    UPDATE trades
                    SET fnc_number = ?
                    WHERE order_id = ?
                      AND (fnc_number IS NULL OR TRIM(fnc_number) = '')
                    """,
                    (docs[0], order_id_value),
                )

        conn.commit()

        # Keep JSON backup in sync so manual assignments survive replacement flows.
        try:
            backup_manual_trades_to_json()
        except Exception as backup_err:
            print(f"[WARN] Trade number saved to DB but backup failed: {backup_err}")

        return True
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error updating trade number: {e}")
    finally:
        conn.close()


def add_rest_trade(trade_data: Dict) -> bool:
    """
    Add a trade from REST API (intraday trades) to database.
    
    Normalizes the trade data from StoneX API format and calculates debit/credit.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Extract key fields from REST API response
        currency_pair = trade_data.get('currency_pair', '')
        last_qty = float(trade_data.get('last_qty', 0) or 0)
        last_px = float(trade_data.get('last_px', 0) or 0)
        rest_trade_id = _normalize_trade_key(trade_data.get('rest_trade_id', ''))
        
        # Determine symbol from currency_pair (e.g., "XAU/USD" -> "XAUUSD")
        symbol = currency_pair.replace('/', '').upper() if currency_pair else ''
        
        # Default to processing XAU and USD pairs
        if not symbol:
            symbol = 'UNKNOWN'
        
        # Determine side from the trade context
        # For REST API, we may need to infer from contract description or other fields
        contract_desc = trade_data.get('contract_description', '').upper()
        side = 'SELL' if 'SELL' in contract_desc else 'BUY'
        
        # Parse dates
        trade_date_int = trade_data.get('trade_date_int', 0)
        transact_time = trade_data.get('transact_time', '')
        process_date = trade_data.get('process_date', '')
        
        # Convert trade_date_int (YYYYMMDD) to date string
        if trade_date_int:
            try:
                trade_date_str = str(trade_date_int)
                trade_date = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:8]}"
            except:
                trade_date = datetime.now().strftime('%Y-%m-%d')
        else:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        # Value date from settlement_date_int
        settlement_date_int = trade_data.get('settlement_date_int', 0)
        if settlement_date_int:
            try:
                settle_str = str(settlement_date_int)
                value_date = f"{settle_str[:4]}-{settle_str[4:6]}-{settle_str[6:8]}"
            except:
                value_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            value_date = (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d')
        
        # Calculate debit/credit based on symbol and side
        debit_usd = 0
        credit_usd = 0
        debit_zar = 0
        credit_zar = 0
        debit_xau = 0
        credit_xau = 0
        
        if symbol == "XAUUSD":
            if side == "SELL":
                debit_xau = last_qty
                credit_usd = last_qty * last_px
            else:  # BUY
                credit_xau = last_qty
                debit_usd = last_qty * last_px
        elif symbol == "USDZAR":
            if side == "SELL":
                debit_usd = last_qty
                credit_zar = last_qty * last_px
            else:  # BUY
                credit_usd = last_qty
                debit_zar = last_qty * last_px
        
        # Settle amount
        settle_amount = last_qty * last_px
        settle_currency = trade_data.get('trade_currency', 'USD')
        
        # Build narration
        narration = f"{symbol} {last_qty:,.2f} @ {last_px:,.4f}"
        
        # Doc number from REST trade ID
        doc_number = rest_trade_id or f"REST-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        clord_id = _normalize_trade_key(trade_data.get("clord_id", "") or trade_data.get("ClOrdId", ""))
        order_id = _normalize_trade_key(trade_data.get("order_id", "") or trade_data.get("OrderId", ""))
        fnc_number = _extract_supporting_doc(
            trade_data.get("neo_id", ""),
            trade_data.get("NeoId", ""),
            trade_data.get("tag_number", ""),
            trade_data.get("TagNumber", ""),
            order_id,
            clord_id,
            trade_data.get("contract_description", ""),
        )
        
        # Get latest balances
        cursor.execute("""
            SELECT balance_usd, balance_zar, balance_xau 
            FROM trades 
            ORDER BY trade_date DESC, created_at DESC 
            LIMIT 1
        """)
        latest_balances = cursor.fetchone()
        
        if latest_balances:
            balance_usd = latest_balances[0] + (credit_usd - debit_usd)
            balance_zar = latest_balances[1] + (credit_zar - debit_zar)
            balance_xau = latest_balances[2] + (credit_xau - debit_xau)
        else:
            balance_usd = credit_usd - debit_usd
            balance_zar = credit_zar - debit_zar
            balance_xau = credit_xau - debit_xau
        
        # Insert into database with all REST API fields
        cursor.execute('''
        INSERT INTO trades (
            trade_date, value_date, symbol, side, narration, quantity, price,
            settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
            debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
            balance_usd, balance_zar, balance_xau,
            rest_trade_id, abs_trade_value, account, account_base_currency, account_id,
            asset_class, contract_description, contract_size, counter_currency,
            currency, currency_pair, last_px, last_qty, process_date,
            settlement_date_int, settlement_price, trade_currency, trade_date_int,
            transact_time, source_system
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade_date, value_date, symbol, side, narration, last_qty, last_px,
            settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
            debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
            balance_usd, balance_zar, balance_xau,
            rest_trade_id,
            float(trade_data.get('abs_trade_value', 0) or 0),
            trade_data.get('account', ''),
            trade_data.get('account_base_currency', ''),
            trade_data.get('account_id', ''),
            trade_data.get('asset_class', ''),
            trade_data.get('contract_description', ''),
            float(trade_data.get('contract_size', 0) or 0),
            trade_data.get('counter_currency', ''),
            trade_data.get('currency', ''),
            currency_pair,
            last_px,
            last_qty,
            process_date,
            settlement_date_int,
            float(trade_data.get('settlement_price', 0) or 0),
            trade_data.get('trade_currency', ''),
            trade_date_int,
            transact_time,
            trade_data.get('source_system', '')
        ))
        
        conn.commit()
        return True
        
    except sqlite3.IntegrityError as e:
        conn.rollback()
        if "UNIQUE constraint failed: trades.doc_number" in str(e):
            # Trade already exists, skip
            return False
        else:
            raise Exception(f"Database integrity error: {e}")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error adding REST trade: {e}")
    finally:
        conn.close()


def insert_murex_trades(murex_df: pd.DataFrame) -> Dict[str, int]:
    """
    Insert StoneX Murex historical trades into the trades table.

    Uses doc_number as a unique key and ignores duplicates.
    """
    if murex_df is None or murex_df.empty:
        return {"inserted": 0, "skipped": 0}

    conn = get_db_connection()
    cursor = conn.cursor()

    def first_non_empty(*vals):
        for v in vals:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s and s.lower() != "nan":
                return s
        return ""

    def parse_yyyymmdd(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        try:
            s = str(int(float(value))).zfill(8)
        except Exception:
            s = str(value).strip()
        if s.isdigit() and len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        try:
            dt = pd.to_datetime(s, errors="coerce")
            return dt.strftime("%Y-%m-%d") if pd.notna(dt) else ""
        except Exception:
            return ""

    def extract_support_doc(row):
        candidates = [
            first_non_empty(row.get("NeoId")),
            first_non_empty(row.get("TagNumber")),
            first_non_empty(row.get("OrderId")),
            first_non_empty(row.get("ClOrdId")),
        ]
        return _extract_supporting_doc(*candidates)

    inserted = 0
    skipped = 0
    doc_support_updates: Dict[str, str] = {}

    # Guard against duplicates where the same doc appears with formatting variants
    # like "45861303" vs "45861303.0".
    cursor.execute("SELECT doc_number FROM trades WHERE doc_number IS NOT NULL AND TRIM(doc_number) != ''")
    existing_docs = {
        _normalize_trade_key(row[0])
        for row in cursor.fetchall()
        if _normalize_trade_key(row[0])
    }
    batch_docs = set()

    for _, r in murex_df.iterrows():
        currency_pair = first_non_empty(r.get("CurrencyPair"))
        symbol = currency_pair.replace("/", "").upper() if currency_pair else first_non_empty(r.get("Symbol")).upper()
        side = first_non_empty(r.get("Side")).upper()

        qty = pd.to_numeric(r.get("LastQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("BaseQuantity"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("BuyQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("SellQty"), errors="coerce")
        qty = float(qty) if pd.notna(qty) else 0.0
        if qty < 0:
            qty = abs(qty)

        price = pd.to_numeric(r.get("LastPx"), errors="coerce")
        if pd.isna(price):
            price = pd.to_numeric(r.get("Price"), errors="coerce")
        price = float(price) if pd.notna(price) else 0.0

        narration = ""
        if currency_pair:
            narration = f"{currency_pair} {qty:,.2f} @ {price:,.4f}"
        else:
            narration = f"{symbol} {qty:,.2f} @ {price:,.4f}".strip()

        trade_date = parse_yyyymmdd(r.get("TradeDate")) or datetime.now().strftime("%Y-%m-%d")
        value_date = parse_yyyymmdd(r.get("ValueDate")) or trade_date

        rest_trade_id = _normalize_trade_key(first_non_empty(r.get("TradeId"), r.get("Id"), r.get("RecId")))
        fnc_number = extract_support_doc(r)
        order_id = ""
        doc_number = _normalize_trade_key(rest_trade_id or order_id)
        if doc_number and fnc_number:
            doc_support_updates[doc_number] = fnc_number
        if not doc_number:
            skipped += 1
            continue
        if doc_number in existing_docs or doc_number in batch_docs:
            skipped += 1
            continue
        batch_docs.add(doc_number)

        clord_id = first_non_empty(r.get("ClOrdId"))

        base = ""
        quote = ""
        if currency_pair and "/" in currency_pair:
            base, quote = currency_pair.split("/", 1)
        elif len(symbol) == 6:
            base, quote = symbol[:3], symbol[3:]

        settle_currency = quote or first_non_empty(r.get("CounterCurrency"), r.get("TradeCurrency"), r.get("Currency"))
        settle_amount = qty * price if qty and price else 0.0

        debit_usd = credit_usd = 0.0
        debit_zar = credit_zar = 0.0

        if base == "USD" and quote == "ZAR":
            if side == "SELL":
                debit_usd = qty
                credit_zar = qty * price
            elif side == "BUY":
                credit_usd = qty
                debit_zar = qty * price
        elif base in {"XAU", "XAG", "XPT", "XPD"} and quote == "USD":
            if side == "BUY":
                debit_usd = qty * price
            elif side == "SELL":
                credit_usd = qty * price

        trader_name = first_non_empty(r.get("MasterAccountPrimaryEmployeeName"), r.get("ContraTrader"))

        try:
            cursor.execute(
                '''
                INSERT OR IGNORE INTO trades (
                    trade_date, value_date, symbol, side, narration, quantity, price,
                    settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
                    debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
                    balance_usd, balance_zar, balance_xau,
                    rest_trade_id, account, account_base_currency, account_id, asset_class,
                    contract_description, contract_size, counter_currency, currency, currency_pair,
                    last_px, last_qty, process_date, settlement_date_int, settlement_price,
                    trade_currency, trade_date_int, transact_time, source_system, trader_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    trade_date, value_date, symbol, side, narration, qty, price,
                    settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
                    debit_usd, credit_usd, debit_zar, credit_zar, 0.0, 0.0,
                    0.0, 0.0, 0.0,
                    rest_trade_id,
                    first_non_empty(r.get("Account"), r.get("AccountNumber")),
                    first_non_empty(r.get("AccountBaseCurrency")),
                    first_non_empty(r.get("AccountId")),
                    first_non_empty(r.get("AssetClass")),
                    first_non_empty(r.get("ContractDescription")),
                    pd.to_numeric(r.get("ContractSize"), errors="coerce"),
                    first_non_empty(r.get("CounterCurrency")),
                    first_non_empty(r.get("Currency")),
                    currency_pair,
                    pd.to_numeric(r.get("LastPx"), errors="coerce"),
                    pd.to_numeric(r.get("LastQty"), errors="coerce"),
                    first_non_empty(r.get("ProcessDate")),
                    pd.to_numeric(r.get("SettlementDate"), errors="coerce"),
                    pd.to_numeric(r.get("SettlementPrice"), errors="coerce"),
                    first_non_empty(r.get("TradeCurrency")),
                    pd.to_numeric(r.get("TradeDate"), errors="coerce"),
                    first_non_empty(r.get("TransactTime")),
                    first_non_empty(r.get("SourceSystem"), "Murex"),
                    trader_name,
                ),
            )
            if cursor.rowcount == 1:
                inserted += 1
                existing_docs.add(doc_number)
            else:
                skipped += 1
        except Exception:
            skipped += 1

    # Backfill known supporting docs for existing rows skipped due duplicate doc_number.
    if doc_support_updates:
        cursor.executemany(
            """
            UPDATE trades
            SET fnc_number = ?
            WHERE doc_number = ?
              AND (fnc_number IS NULL OR TRIM(fnc_number) = '')
            """,
            [(doc, doc_no) for doc_no, doc in doc_support_updates.items()],
        )

    # Backfill supporting doc from OrderID when it carries FNC/SWT tokens.
    cursor.execute("""
        UPDATE trades
        SET fnc_number = order_id
        WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
          AND (
                order_id LIKE 'FNC/%'
             OR order_id LIKE 'SWT/%'
             OR order_id LIKE 'FCT/%'
          )
    """)
    conn.commit()
    conn.close()

    restored = 0
    try:
        restore_result = restore_manual_trades_from_json(only_blank=True)
        restored = restore_result.get("restored", 0)
    except Exception as exc:
        print(f"[WARN] insert_murex_trades: restore from backup failed: {exc}")

    return {"inserted": inserted, "skipped": skipped, "restored_order_ids": restored}


def replace_trades_with_murex(murex_df: pd.DataFrame) -> Dict[str, int]:
    """
    Replace all trades in the database with StoneX Murex historical trades.

    This deletes existing rows in trades and inserts the Murex dataset.
    """
    if murex_df is None or murex_df.empty:
        return {"inserted": 0, "skipped": 0}

    conn = get_db_connection()
    cursor = conn.cursor()

    def first_non_empty(*vals):
        for v in vals:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s and s.lower() != "nan":
                return s
        return ""

    def parse_yyyymmdd(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        try:
            s = str(int(float(value))).zfill(8)
        except Exception:
            s = str(value).strip()
        if s.isdigit() and len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        try:
            dt = pd.to_datetime(s, errors="coerce")
            return dt.strftime("%Y-%m-%d") if pd.notna(dt) else ""
        except Exception:
            return ""

    def extract_support_doc(row):
        candidates = [
            first_non_empty(row.get("NeoId")),
            first_non_empty(row.get("TagNumber")),
            first_non_empty(row.get("OrderId")),
            first_non_empty(row.get("ClOrdId")),
        ]
        return _extract_supporting_doc(*candidates)

    # 1. Backup existing manual overrides from Database
    cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
    preserved_ids_db = cursor.fetchall()
    print(f"[DEBUG] replace_trades_with_murex: Preserved {len(preserved_ids_db)} manual trade IDs from DB")
    if preserved_ids_db:
        print(f"[DEBUG] Sample preserved DB: {preserved_ids_db[:3]}")

    # 1b. Load manual overrides from JSON file (Explicit Save)
    preserved_ids_file = {}
    if os.path.exists(MANUAL_TRADES_FILE):
        try:
            with open(MANUAL_TRADES_FILE, 'r') as f:
                preserved_ids_file = json.load(f)
            print(f"[DEBUG] replace_trades_with_murex: Loaded {len(preserved_ids_file)} manual trade IDs from file")
        except Exception as e:
            print(f"[ERROR] Failed to load manual trades file: {e}")

    # Merge: DB takes precedence locally, but usually they should match.
    # Actually, if DB is about to be wiped, we want to ensure we have EVERYTHING.
    # Let's use a dict to merge.
    merged_backup = {
        _normalize_trade_key(doc): _normalize_trade_key(oid)
        for doc, oid in preserved_ids_file.items()
        if _normalize_trade_key(doc) and _normalize_trade_key(oid)
    }
    for doc, oid in preserved_ids_db:
        doc_key = _normalize_trade_key(doc)
        order_key = _normalize_trade_key(oid)
        if doc_key and order_key:
            merged_backup[doc_key] = order_key # DB overrides file (more recent edits not yet saved to file?)
    
    # Convert back to list of tuples for restore
    preserved_ids = list(merged_backup.items())
    print(f"[DEBUG] replace_trades_with_murex: Total merged IDs to restore: {len(preserved_ids)}")

    # 2. Clear existing trades
    cursor.execute("DELETE FROM trades")

    inserted = 0
    skipped = 0
    seen_docs = set()

    for _, r in murex_df.iterrows():
        currency_pair = first_non_empty(r.get("CurrencyPair"))
        symbol = currency_pair.replace("/", "").upper() if currency_pair else first_non_empty(r.get("Symbol")).upper()
        side = first_non_empty(r.get("Side")).upper()

        if not symbol or not side:
            skipped += 1
            continue

        qty = pd.to_numeric(r.get("LastQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("BaseQuantity"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("BuyQty"), errors="coerce")
        if pd.isna(qty):
            qty = pd.to_numeric(r.get("SellQty"), errors="coerce")
        qty = float(qty) if pd.notna(qty) else 0.0
        if qty < 0:
            qty = abs(qty)

        price = pd.to_numeric(r.get("LastPx"), errors="coerce")
        if pd.isna(price):
            price = pd.to_numeric(r.get("Price"), errors="coerce")
        price = float(price) if pd.notna(price) else 0.0

        narration = ""
        if currency_pair:
            narration = f"{currency_pair} {qty:,.2f} @ {price:,.4f}"
        else:
            narration = f"{symbol} {qty:,.2f} @ {price:,.4f}".strip()

        trade_date = parse_yyyymmdd(r.get("TradeDate")) or datetime.now().strftime("%Y-%m-%d")
        value_date = parse_yyyymmdd(r.get("ValueDate")) or trade_date

        rest_trade_id = _normalize_trade_key(first_non_empty(r.get("TradeId"), r.get("Id"), r.get("RecId")))
        fnc_number = extract_support_doc(r)
        order_id = ""
        doc_number = _normalize_trade_key(rest_trade_id or order_id)

        if not doc_number or doc_number in seen_docs:
            skipped += 1
            continue
        seen_docs.add(doc_number)

        clord_id = first_non_empty(r.get("ClOrdId"))

        base = ""
        quote = ""
        if currency_pair and "/" in currency_pair:
            base, quote = currency_pair.split("/", 1)
        elif len(symbol) == 6:
            base, quote = symbol[:3], symbol[3:]

        settle_currency = quote or first_non_empty(r.get("CounterCurrency"), r.get("TradeCurrency"), r.get("Currency"), "USD")
        settle_amount = qty * price if qty and price else 0.0

        debit_usd = credit_usd = 0.0
        debit_zar = credit_zar = 0.0

        if base == "USD" and quote == "ZAR":
            if side == "SELL":
                debit_usd = qty
                credit_zar = qty * price
            elif side == "BUY":
                credit_usd = qty
                debit_zar = qty * price
        elif base in {"XAU", "XAG", "XPT", "XPD"} and quote == "USD":
            if side == "BUY":
                debit_usd = qty * price
            elif side == "SELL":
                credit_usd = qty * price

        trader_name = first_non_empty(r.get("MasterAccountPrimaryEmployeeName"), r.get("ContraTrader"))

        try:
            cursor.execute(
                '''
                INSERT INTO trades (
                    trade_date, value_date, symbol, side, narration, quantity, price,
                    settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
                    debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
                    balance_usd, balance_zar, balance_xau,
                    rest_trade_id, account, account_base_currency, account_id, asset_class,
                    contract_description, contract_size, counter_currency, currency, currency_pair,
                    last_px, last_qty, process_date, settlement_date_int, settlement_price,
                    trade_currency, trade_date_int, transact_time, source_system, trader_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    trade_date, value_date, symbol, side, narration, qty, price,
                    settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
                    debit_usd, credit_usd, debit_zar, credit_zar, 0.0, 0.0,
                    0.0, 0.0, 0.0,
                    rest_trade_id,
                    first_non_empty(r.get("Account"), r.get("AccountNumber")),
                    first_non_empty(r.get("AccountBaseCurrency")),
                    first_non_empty(r.get("AccountId")),
                    first_non_empty(r.get("AssetClass")),
                    first_non_empty(r.get("ContractDescription")),
                    pd.to_numeric(r.get("ContractSize"), errors="coerce"),
                    first_non_empty(r.get("CounterCurrency")),
                    first_non_empty(r.get("Currency")),
                    currency_pair,
                    pd.to_numeric(r.get("LastPx"), errors="coerce"),
                    pd.to_numeric(r.get("LastQty"), errors="coerce"),
                    first_non_empty(r.get("ProcessDate")),
                    pd.to_numeric(r.get("SettlementDate"), errors="coerce"),
                    pd.to_numeric(r.get("SettlementPrice"), errors="coerce"),
                    first_non_empty(r.get("TradeCurrency")),
                    pd.to_numeric(r.get("TradeDate"), errors="coerce"),
                    first_non_empty(r.get("TransactTime")),
                    first_non_empty(r.get("SourceSystem"), "Murex"),
                    trader_name,
                ),
            )
            inserted += 1
        except Exception:
            skipped += 1

    # Backfill supporting doc from OrderID when it carries FNC/SWT tokens.
    cursor.execute("""
        UPDATE trades
        SET fnc_number = order_id
        WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
          AND (
                order_id LIKE 'FNC/%'
             OR order_id LIKE 'SWT/%'
             OR order_id LIKE 'FCT/%'
          )
    """)

    # 3. Restore manual overrides (Trade Numbers)
    if preserved_ids:
        cursor.execute("SELECT id, doc_number FROM trades")
        inserted_rows = cursor.fetchall()
        trade_ids_by_doc = {}
        for trade_id, raw_doc in inserted_rows:
            doc_key = _normalize_trade_key(raw_doc)
            if doc_key:
                trade_ids_by_doc.setdefault(doc_key, []).append(trade_id)

        restore_updates = []
        for doc, oid in preserved_ids:
            doc_key = _normalize_trade_key(doc)
            order_key = _normalize_trade_key(oid)
            if not doc_key or not order_key:
                continue
            for trade_id in trade_ids_by_doc.get(doc_key, []):
                restore_updates.append((order_key, trade_id))

        if restore_updates:
            cursor.executemany("UPDATE trades SET order_id = ? WHERE id = ?", restore_updates)
            print(f"[DEBUG] replace_trades_with_murex: Restored {len(restore_updates)} manual trade IDs")

    conn.commit()
    conn.close()

    restored = 0
    try:
        restore_result = restore_manual_trades_from_json(only_blank=True)
        restored = restore_result.get("restored", 0)
    except Exception as exc:
        print(f"[WARN] replace_trades_with_murex: restore from backup failed: {exc}")

    return {"inserted": inserted, "skipped": skipped, "restored_order_ids": restored}


def restore_manual_trades_from_json(only_blank: bool = True) -> Dict[str, int]:
    """Restore saved manual trade assignments from JSON into the trades table."""
    if not os.path.exists(MANUAL_TRADES_FILE):
        return {"restored": 0, "available": 0}

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        with open(MANUAL_TRADES_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"restored": 0, "available": 0}

        updates = []
        for doc_number, order_id in data.items():
            doc_key = _normalize_trade_key(doc_number)
            order_key = _normalize_trade_key(order_id)
            if not doc_key or not order_key:
                continue
            updates.append((order_key, doc_key))

        if not updates:
            return {"restored": 0, "available": 0}

        cursor.execute("SELECT id, doc_number, order_id FROM trades")
        db_rows = cursor.fetchall()
        trade_ids_by_doc = {}
        for trade_id, raw_doc, raw_order in db_rows:
            doc_key = _normalize_trade_key(raw_doc)
            if not doc_key:
                continue
            current_order = _normalize_trade_key(raw_order)
            trade_ids_by_doc.setdefault(doc_key, []).append((trade_id, current_order))

        id_updates = []
        for order_key, doc_key in updates:
            matches = trade_ids_by_doc.get(doc_key, [])
            if not matches:
                continue
            for trade_id, current_order in matches:
                if only_blank and current_order:
                    continue
                id_updates.append((order_key, trade_id))

        if not id_updates:
            return {"restored": 0, "available": len(updates)}

        before = conn.total_changes
        cursor.executemany(
            "UPDATE trades SET order_id = ? WHERE id = ?",
            id_updates,
        )

        conn.commit()
        restored = conn.total_changes - before
        return {"restored": restored, "available": len(updates)}
    except Exception as e:
        print(f"[ERROR] Failed to restore manual trades: {e}")
        return {"restored": 0, "available": 0}
    finally:
        conn.close()


def backup_manual_trades_to_json() -> bool:
    """Save all manual trade assignments to a JSON file for persistence."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
        rows = cursor.fetchall()
        
        data = {}
        for doc_number, order_id in rows:
            doc_key = _normalize_trade_key(doc_number)
            order_key = _normalize_trade_key(order_id)
            if doc_key and order_key:
                data[doc_key] = order_key
        
        with open(MANUAL_TRADES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
            
        print(f"[INFO] Backed up {len(data)} manual trades to {MANUAL_TRADES_FILE}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to backup manual trades: {e}")
        return False
    finally:
        conn.close()
