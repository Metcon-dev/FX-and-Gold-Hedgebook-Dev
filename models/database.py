"""Database connection and initialization"""
import os
import sqlite3
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SHARED_DB_ROOT = r"T:\Trading Platform - db"
DB_PATH = os.path.join(SHARED_DB_ROOT, "fx_trading_ledger.db")
os.environ["LEDGER_DB_PATH"] = DB_PATH


def get_db_connection():
    """Create database connection"""
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        # Best-effort pragmas; continue even if they fail
        pass
    return conn


def initialize_database():
    """Create the trades table if it doesn't exist, and add missing columns"""
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            break
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() and attempt < retries:
                time.sleep(1)
                continue
            raise
    
    # First create the main table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date DATE NOT NULL,
        value_date DATE NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        narration TEXT,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        settle_currency TEXT NOT NULL,
        settle_amount REAL NOT NULL,
        doc_number TEXT UNIQUE,
        clord_id TEXT,
        order_id TEXT,
        fnc_number TEXT,
        debit_usd REAL DEFAULT 0,
        credit_usd REAL DEFAULT 0,
        debit_zar REAL DEFAULT 0,
        credit_zar REAL DEFAULT 0,
        debit_xau REAL DEFAULT 0,
        credit_xau REAL DEFAULT 0,
        balance_usd REAL DEFAULT 0,
        balance_zar REAL DEFAULT 0,
        balance_xau REAL DEFAULT 0,
        
        -- FIX API FIELDS
        fix_message TEXT,
        fix_trade_id TEXT,
        fix_clord_id TEXT,
        fix_exec_id TEXT,
        fix_account TEXT,
        fix_settlement_date TEXT,
        fix_transact_time TEXT,
        fix_report_type TEXT,
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Now check for missing columns and add them
    columns_to_add = [
        ('fix_message', 'TEXT'),
        ('fix_trade_id', 'TEXT'),
        ('fix_clord_id', 'TEXT'),
        ('fix_exec_id', 'TEXT'),
        ('fix_account', 'TEXT'),
        ('fix_settlement_date', 'TEXT'),
        ('fix_transact_time', 'TEXT'),
        ('fix_report_type', 'TEXT'),
        ('trader_name', 'TEXT'),
        ('fix_trans_type', 'TEXT'),
        ('fnc_number', 'TEXT'),
        # Additional metal types
        ('debit_xag', 'REAL DEFAULT 0'),
        ('credit_xag', 'REAL DEFAULT 0'),
        ('balance_xag', 'REAL DEFAULT 0'),
        ('debit_xpt', 'REAL DEFAULT 0'),
        ('credit_xpt', 'REAL DEFAULT 0'),
        ('balance_xpt', 'REAL DEFAULT 0'),
        ('debit_xpd', 'REAL DEFAULT 0'),
        ('credit_xpd', 'REAL DEFAULT 0'),
        ('balance_xpd', 'REAL DEFAULT 0'),
        # REST API Intraday Trade fields
        ('rest_trade_id', 'TEXT'),
        ('abs_trade_value', 'REAL'),
        ('account', 'TEXT'),
        ('account_base_currency', 'TEXT'),
        ('account_id', 'TEXT'),
        ('asset_class', 'TEXT'),
        ('contract_description', 'TEXT'),
        ('contract_size', 'REAL'),
        ('counter_currency', 'TEXT'),
        ('currency', 'TEXT'),
        ('currency_pair', 'TEXT'),
        ('last_px', 'REAL'),
        ('last_qty', 'REAL'),
        ('process_date', 'TEXT'),
        ('settlement_date_int', 'INTEGER'),
        ('settlement_price', 'REAL'),
        ('trade_currency', 'TEXT'),
        ('trade_date_int', 'INTEGER'),
        ('transact_time', 'TEXT'),
        ('source_system', 'TEXT'),
    ]
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(trades)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    # Add missing columns
    for column_name, column_type in columns_to_add:
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE trades ADD COLUMN {column_name} {column_type}")
    
    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')

    # Backfill support doc from OrderID when available
    if 'fnc_number' in existing_columns or 'fnc_number' in [c[0] for c in columns_to_add]:
        cursor.execute("""
            UPDATE trades
            SET fnc_number = order_id
            WHERE (fnc_number IS NULL OR fnc_number = '')
              AND (
                    order_id LIKE 'FNC/%'
                 OR order_id LIKE 'SWT/%'
                 OR order_id LIKE 'FCT/%'
              )
        """)
    
    conn.commit()
    conn.close()

