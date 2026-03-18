# App Data Read/Write and API Call Documentation

Generated: 2026-03-17 10:23:41

This document includes code snippets for:
- Database read/write call sites
- Outbound HTTP/API calls
- Flask API route surface

## Database Read/Write Call Sites
### j2-platform/server\server.py

#### Line 413
```python
   411:     if not lookup:
   412:         return None
   413:     conn = get_db_connection()
   414:     cur = conn.cursor()
   415:     cur.execute(
   416:         f"""
   417:         SELECT id, username, display_name, password_hash, role, can_read, can_write, is_admin, is_active, created_at
```

#### Line 432
```python
   430:     if int(user_id or 0) <= 0:
   431:         return None
   432:     conn = get_db_connection()
   433:     cur = conn.cursor()
   434:     cur.execute(
   435:         f"""
   436:         SELECT id, username, display_name, password_hash, role, can_read, can_write, is_admin, is_active, created_at
```

#### Line 450
```python
   448: def _auth_list_users() -> List[Dict[str, Any]]:
   449:     """List all app users ordered by username."""
   450:     conn = get_db_connection()
   451:     cur = conn.cursor()
   452:     cur.execute(
   453:         f"""
   454:         SELECT id, username, display_name, password_hash, role, can_read, can_write, is_admin, is_active, created_at
```

#### Line 495
```python
   493:     force_reseed = _pmx_bool(os.getenv("APP_AUTH_FORCE_RESEED", "false"), default=False)
   494: 
   495:     conn = get_db_connection()
   496:     cur = conn.cursor()
   497:     cur.execute(
   498:         f"""
   499:         CREATE TABLE IF NOT EXISTS {AUTH_USER_TABLE} (
```

#### Line 659
```python
   657:         )
   658: 
   659:     commit_with_mirror(conn)
   660:     conn.close()
   661: 
   662: 
   663: def _auth_cookie_token() -> str:
```

#### Line 1448
```python
  1446: 
  1447: 
  1448: def get_pmx_db_connection():
  1449:     """Create SQLite connection for PMX ledger database."""
  1450:     conn = sqlite3.connect(PMX_DB_PATH, timeout=30, check_same_thread=False)
  1451:     try:
  1452:         conn.execute("PRAGMA journal_mode=WAL;")
```

#### Line 1450
```python
  1448: def get_pmx_db_connection():
  1449:     """Create SQLite connection for PMX ledger database."""
  1450:     conn = sqlite3.connect(PMX_DB_PATH, timeout=30, check_same_thread=False)
  1451:     try:
  1452:         conn.execute("PRAGMA journal_mode=WAL;")
  1453:         conn.execute("PRAGMA synchronous=NORMAL;")
  1454:         conn.execute("PRAGMA busy_timeout=5000;")
```

#### Line 1490
```python
  1488: def _pmx_delete_swap_rows(conn: sqlite3.Connection) -> int:
  1489:     cursor = conn.cursor()
  1490:     cursor.execute(f"DELETE FROM trades WHERE {_pmx_swap_sql_predicate()}")
  1491:     return max(int(cursor.rowcount or 0), 0)
  1492: 
  1493: 
  1494: def _purge_pre_fiscal_rows(
```

#### Line 1511
```python
  1509: 
  1510:     if purge_fx:
  1511:         conn_fx = sqlite3.connect(LEDGER_DB_PATH, timeout=30, check_same_thread=False)
  1512:         try:
  1513:             cur_fx = conn_fx.cursor()
  1514:             cur_fx.execute(
  1515:                 """
```

#### Line 1553
```python
  1551: 
  1552:     if purge_pmx:
  1553:         conn_pmx = get_pmx_db_connection()
  1554:         try:
  1555:             cur_pmx = conn_pmx.cursor()
  1556:             cur_pmx.execute(
  1557:                 """
```

#### Line 1573
```python
  1571: def initialize_pmx_database():
  1572:     """Initialize PMX trades database (separate from main ledger DB)."""
  1573:     conn = get_pmx_db_connection()
  1574:     cur = conn.cursor()
  1575:     cur.execute(
  1576:         """
  1577:         CREATE TABLE IF NOT EXISTS trades (
```

#### Line 1624
```python
  1622:     cur.execute("CREATE INDEX IF NOT EXISTS idx_pmx_fnc_number ON trades(fnc_number)")
  1623:     removed_swaps = _pmx_delete_swap_rows(conn)
  1624:     commit_with_mirror(conn)
  1625:     if removed_swaps > 0:
  1626:         print(f"[PMX] Removed {removed_swaps} historical SWT/SWAP rows from PMX DB during startup.")
  1627:     conn.close()
  1628: 
```

#### Line 1632
```python
  1630: def initialize_account_opening_balances_table():
  1631:     """Initialize the account_opening_balances table for reconciliation opening balances."""
  1632:     conn = get_pmx_db_connection()
  1633:     cur = conn.cursor()
  1634:     cur.execute("""
  1635:         CREATE TABLE IF NOT EXISTS account_opening_balances (
  1636:             id INTEGER PRIMARY KEY AUTOINCREMENT,
```

#### Line 1644
```python
  1642:         )
  1643:     """)
  1644:     commit_with_mirror(conn)
  1645:     conn.close()
  1646: 
  1647: 
  1648: initialize_pmx_database()
```

#### Line 2082
```python
  2080: 
  2081: def _get_latest_pmx_trade_date() -> Optional[str]:
  2082:     conn = get_pmx_db_connection()
  2083:     try:
  2084:         cursor = conn.cursor()
  2085:         cursor.execute(
  2086:             f"""
```

#### Line 2085
```python
  2083:     try:
  2084:         cursor = conn.cursor()
  2085:         cursor.execute(
  2086:             f"""
  2087:             SELECT MAX(trade_date)
  2088:             FROM trades
  2089:             WHERE trade_date IS NOT NULL
```

#### Line 2120
```python
  2118: 
  2119: def load_all_pmx_trades(filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
  2120:     conn = get_pmx_db_connection()
  2121:     try:
  2122:         where_clauses = [f"NOT {_pmx_swap_sql_predicate()}"]
  2123:         params: List[Any] = []
  2124: 
```

#### Line 2193
```python
  2191:             ORDER BY trade_date ASC, id ASC
  2192:         """.format(where_sql=where_sql)
  2193:         return pd.read_sql_query(query, conn, params=params)
  2194:     finally:
  2195:         conn.close()
  2196: 
  2197: 
```

#### Line 2199
```python
  2197: 
  2198: def update_pmx_trade_order_id(trade_id: int, order_id: str) -> bool:
  2199:     conn = get_pmx_db_connection()
  2200:     cursor = conn.cursor()
  2201:     try:
  2202:         order_id_value = normalize_trade_number(order_id) if order_id and str(order_id).strip() else None
  2203:         cursor.execute(
```

#### Line 2203
```python
  2201:     try:
  2202:         order_id_value = normalize_trade_number(order_id) if order_id and str(order_id).strip() else None
  2203:         cursor.execute(
  2204:             """
  2205:             UPDATE trades
  2206:             SET order_id = ?
  2207:             WHERE id = ?
```

#### Line 2212
```python
  2210:         )
  2211:         if cursor.rowcount == 0:
  2212:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
  2213:             if cursor.fetchone() is None:
  2214:                 conn.rollback()
  2215:                 return False
  2216:         commit_with_mirror(conn)
```

#### Line 2214
```python
  2212:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
  2213:             if cursor.fetchone() is None:
  2214:                 conn.rollback()
  2215:                 return False
  2216:         commit_with_mirror(conn)
  2217:         return True
  2218:     except Exception:
```

#### Line 2216
```python
  2214:                 conn.rollback()
  2215:                 return False
  2216:         commit_with_mirror(conn)
  2217:         return True
  2218:     except Exception:
  2219:         conn.rollback()
  2220:         raise
```

#### Line 2219
```python
  2217:         return True
  2218:     except Exception:
  2219:         conn.rollback()
  2220:         raise
  2221:     finally:
  2222:         conn.close()
  2223: 
```

#### Line 2232
```python
  2230:     conn = None
  2231:     try:
  2232:         conn = get_pmx_db_connection() if use_pmx else get_db_connection()
  2233:         row = conn.execute("SELECT symbol FROM trades WHERE id = ?", (int(trade_id),)).fetchone()
  2234:         if not row:
  2235:             return None
  2236:         return str(row[0] or "")
```

#### Line 2270
```python
  2268:     conn = None
  2269:     try:
  2270:         conn = get_db_connection()
  2271:         rows = pd.read_sql_query(
  2272:             """
  2273:             SELECT
  2274:                 ref_number,
```

#### Line 2271
```python
  2269:     try:
  2270:         conn = get_db_connection()
  2271:         rows = pd.read_sql_query(
  2272:             """
  2273:             SELECT
  2274:                 ref_number,
  2275:                 trade_timestamp,
```

#### Line 2630
```python
  2628:     """
  2629: 
  2630:     conn = get_pmx_db_connection()
  2631:     cursor = conn.cursor()
  2632:     try:
  2633:         if replace:
  2634:             cursor.execute("DELETE FROM trades")
```

#### Line 2634
```python
  2632:     try:
  2633:         if replace:
  2634:             cursor.execute("DELETE FROM trades")
  2635: 
  2636:         inserted = 0
  2637:         updated = 0
  2638:         skipped = 0
```

#### Line 2656
```python
  2654:                 continue
  2655: 
  2656:             cursor.execute("SELECT 1 FROM trades WHERE doc_number = ?", (mapped["doc_number"],))
  2657:             exists = cursor.fetchone() is not None
  2658:             values = tuple(mapped[col] for col in insert_cols)
  2659:             cursor.execute(insert_sql, values)
  2660:             if exists:
```

#### Line 2659
```python
  2657:             exists = cursor.fetchone() is not None
  2658:             values = tuple(mapped[col] for col in insert_cols)
  2659:             cursor.execute(insert_sql, values)
  2660:             if exists:
  2661:                 updated += 1
  2662:             else:
  2663:                 inserted += 1
```

#### Line 2665
```python
  2663:                 inserted += 1
  2664: 
  2665:         commit_with_mirror(conn)
  2666:         out["inserted"] = inserted
  2667:         out["updated"] = updated
  2668:         out["skipped"] = skipped
  2669:         out["skipped_swaps"] = skipped_swaps
```

#### Line 2675
```python
  2673:         out["fiscal_cutoff"] = FISCAL_TRADES_START_DATE
  2674:     except Exception as exc:
  2675:         conn.rollback()
  2676:         out["ok"] = False
  2677:         out["error"] = str(exc)
  2678:     finally:
  2679:         conn.close()
```

#### Line 2855
```python
  2853:     import sqlite3
  2854:     db_path = os.path.join(ORIGINAL_PROJECT, "fx_trading_ledger.db")
  2855:     conn = sqlite3.connect(db_path)
  2856:     try:
  2857:         df = pd.read_sql_query("SELECT * FROM trademc_companies ORDER BY company_name", conn)
  2858:     except Exception:
  2859:         df = pd.DataFrame()
```

#### Line 2857
```python
  2855:     conn = sqlite3.connect(db_path)
  2856:     try:
  2857:         df = pd.read_sql_query("SELECT * FROM trademc_companies ORDER BY company_name", conn)
  2858:     except Exception:
  2859:         df = pd.DataFrame()
  2860:     conn.close()
  2861:     return df
```

#### Line 2868
```python
  2866:     import sqlite3
  2867:     db_path = os.path.join(ORIGINAL_PROJECT, "fx_trading_ledger.db")
  2868:     conn = sqlite3.connect(db_path)
  2869:     query = """
  2870:         SELECT t.*, c.company_name, c.refining_rate AS company_refining_rate
  2871:         FROM trademc_trades t
  2872:         LEFT JOIN trademc_companies c ON t.company_id = c.id
```

#### Line 2897
```python
  2895:         params.append(end_date)
  2896:     query += " ORDER BY t.trade_timestamp DESC"
  2897:     df = pd.read_sql_query(query, conn, params=params)
  2898:     conn.close()
  2899:     for col in ["trade_timestamp", "date_created", "date_updated", "last_synced"]:
  2900:         if col in df.columns:
  2901:             df[col] = pd.to_datetime(df[col], errors="coerce")
```

#### Line 2909
```python
  2907:     import sqlite3
  2908:     db_path = os.path.join(ORIGINAL_PROJECT, "fx_trading_ledger.db")
  2909:     conn = sqlite3.connect(db_path)
  2910:     query = """
  2911:         SELECT w.*, c.company_name
  2912:         FROM trademc_weight_transactions w
  2913:         LEFT JOIN trademc_companies c ON w.company_id = c.id
```

#### Line 2934
```python
  2932:         params.append(end_date)
  2933:     query += " ORDER BY w.transaction_timestamp DESC"
  2934:     df = pd.read_sql_query(query, conn, params=params)
  2935:     conn.close()
  2936:     return df
  2937: 
  2938: 
```

#### Line 3845
```python
  3843:     opening: Dict[str, Optional[float]] = {}
  3844:     try:
  3845:         conn = get_pmx_db_connection()
  3846:         cur = conn.cursor()
  3847:         cur.execute(
  3848:             "SELECT currency, opening_balance FROM account_opening_balances WHERE month = ?",
  3849:             (month,),
```

#### Line 3922
```python
  3920: 
  3921: 
  3922: def _daily_balance_email_get_db_connection() -> sqlite3.Connection:
  3923:     conn = sqlite3.connect(LEDGER_DB_PATH, timeout=30, check_same_thread=False)
  3924:     try:
  3925:         conn.execute("PRAGMA journal_mode=WAL;")
  3926:         conn.execute("PRAGMA synchronous=NORMAL;")
```

#### Line 3923
```python
  3921: 
  3922: def _daily_balance_email_get_db_connection() -> sqlite3.Connection:
  3923:     conn = sqlite3.connect(LEDGER_DB_PATH, timeout=30, check_same_thread=False)
  3924:     try:
  3925:         conn.execute("PRAGMA journal_mode=WAL;")
  3926:         conn.execute("PRAGMA synchronous=NORMAL;")
  3927:         conn.execute("PRAGMA busy_timeout=5000;")
```

#### Line 3934
```python
  3932: 
  3933: def _daily_balance_email_ensure_log_table() -> None:
  3934:     conn = _daily_balance_email_get_db_connection()
  3935:     try:
  3936:         cur = conn.cursor()
  3937:         cur.execute(
  3938:             """
```

#### Line 3955
```python
  3953:             """
  3954:         )
  3955:         commit_with_mirror(conn)
  3956:     finally:
  3957:         conn.close()
  3958: 
  3959: 
```

#### Line 3961
```python
  3959: 
  3960: def _daily_balance_email_log_run(job_name: str, run_date: str, status: str, message: str = "") -> None:
  3961:     conn = _daily_balance_email_get_db_connection()
  3962:     try:
  3963:         cur = conn.cursor()
  3964:         cur.execute(
  3965:             """
```

#### Line 3977
```python
  3975:             ),
  3976:         )
  3977:         commit_with_mirror(conn)
  3978:     finally:
  3979:         conn.close()
  3980: 
  3981: 
```

#### Line 3983
```python
  3981: 
  3982: def _daily_balance_email_has_success(job_name: str, run_date: str) -> bool:
  3983:     conn = _daily_balance_email_get_db_connection()
  3984:     try:
  3985:         cur = conn.cursor()
  3986:         cur.execute(
  3987:             """
```

#### Line 4634
```python
  4632:     ledger_conn = None
  4633:     try:
  4634:         tm_conn = get_db_connection()
  4635:         tm = pd.read_sql_query(
  4636:             """
  4637:             SELECT
  4638:                 ref_number,
```

#### Line 4635
```python
  4633:     try:
  4634:         tm_conn = get_db_connection()
  4635:         tm = pd.read_sql_query(
  4636:             """
  4637:             SELECT
  4638:                 ref_number,
  4639:                 weight
```

#### Line 4647
```python
  4645:             tm_conn,
  4646:         )
  4647:         ledger_conn = get_pmx_db_connection() if use_pmx else get_db_connection()
  4648:         ledger = pd.read_sql_query(
  4649:             """
  4650:             SELECT
  4651:                 order_id AS OrderID,
```

#### Line 4648
```python
  4646:         )
  4647:         ledger_conn = get_pmx_db_connection() if use_pmx else get_db_connection()
  4648:         ledger = pd.read_sql_query(
  4649:             """
  4650:             SELECT
  4651:                 order_id AS OrderID,
  4652:                 symbol AS Symbol,
```

#### Line 6889
```python
  6887:         can_read = True
  6888: 
  6889:     conn = get_db_connection()
  6890:     cur = conn.cursor()
  6891:     cur.execute(
  6892:         f"""
  6893:         SELECT id
```

#### Line 6924
```python
  6922:     )
  6923:     created_id = int(cur.lastrowid or 0)
  6924:     commit_with_mirror(conn)
  6925:     conn.close()
  6926: 
  6927:     created = _auth_find_user_by_id(created_id)
  6928:     if not created:
```

#### Line 6965
```python
  6963:     will_be_admin_active = bool(is_admin) and bool(is_active)
  6964:     if was_admin_active and not will_be_admin_active:
  6965:         conn = get_db_connection()
  6966:         cur = conn.cursor()
  6967:         cur.execute(
  6968:             f"""
  6969:             SELECT COUNT(*)
```

#### Line 6989
```python
  6987:         password_hash = _auth_hash_password(password)
  6988: 
  6989:     conn = get_db_connection()
  6990:     cur = conn.cursor()
  6991:     cur.execute(
  6992:         f"""
  6993:         SELECT id
```

#### Line 7045
```python
  7043:         )
  7044: 
  7045:     commit_with_mirror(conn)
  7046:     conn.close()
  7047: 
  7048:     updated = _auth_find_user_by_id(user_id)
  7049:     if not updated:
```

#### Line 7067
```python
  7065:         return jsonify({"ok": False, "error": "You cannot delete your own account"}), 400
  7066: 
  7067:     conn = get_db_connection()
  7068:     cur = conn.cursor()
  7069: 
  7070:     if bool(target.get("is_admin")) and bool(target.get("is_active")):
  7071:         cur.execute(
```

#### Line 7091
```python
  7089:         (int(user_id),),
  7090:     )
  7091:     commit_with_mirror(conn)
  7092:     conn.close()
  7093: 
  7094:     return jsonify({"ok": True, "deleted_id": int(user_id)})
  7095: 
```

#### Line 9556
```python
  9554:         return jsonify({"ok": False, "error": "opening_balance must be a number"}), 400
  9555:     try:
  9556:         conn = get_pmx_db_connection()
  9557:         cur = conn.cursor()
  9558:         cur.execute(
  9559:             """
  9560:             INSERT INTO account_opening_balances (month, currency, opening_balance, updated_at)
```

#### Line 9566
```python
  9564:             (month, currency, opening_balance, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
  9565:         )
  9566:         commit_with_mirror(conn)
  9567:         conn.close()
  9568:     except Exception as exc:
  9569:         return jsonify({"ok": False, "error": str(exc)}), 500
  9570:     # Invalidate cached recon results
```

#### Line 9578
```python
  9576: def get_pmx_account_recon_opening_balances():
  9577:     try:
  9578:         conn = get_pmx_db_connection()
  9579:         cur = conn.cursor()
  9580:         cur.execute("SELECT id, month, currency, opening_balance, updated_at FROM account_opening_balances ORDER BY month DESC")
  9581:         rows = [
  9582:             {"id": r[0], "month": r[1], "currency": r[2], "opening_balance": r[3], "updated_at": r[4]}
```

### models\database.py

#### Line 14
```python
    12: 
    13: 
    14: def get_db_connection():
    15:     """Create database connection"""
    16:     conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    17:     try:
    18:         conn.execute("PRAGMA journal_mode=WAL;")
```

#### Line 16
```python
    14: def get_db_connection():
    15:     """Create database connection"""
    16:     conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    17:     try:
    18:         conn.execute("PRAGMA journal_mode=WAL;")
    19:         conn.execute("PRAGMA synchronous=NORMAL;")
    20:         conn.execute("PRAGMA busy_timeout=5000;")
```

#### Line 56
```python
    54:     if parent:
    55:         os.makedirs(parent, exist_ok=True)
    56:     target_conn = sqlite3.connect(target_path, timeout=30, check_same_thread=False)
    57:     try:
    58:         source_conn.backup(target_conn)
    59:     finally:
    60:         target_conn.close()
```

#### Line 63
```python
    61: 
    62: 
    63: def commit_with_mirror(conn: sqlite3.Connection, db_path: str = "") -> None:
    64:     """
    65:     Commit the active transaction, then mirror the database to configured target path(s).
    66:     Mirror errors are logged but do not raise.
    67:     """
```

#### Line 88
```python
    86:     for attempt in range(1, retries + 1):
    87:         try:
    88:             conn = get_db_connection()
    89:             cursor = conn.cursor()
    90:             break
    91:         except sqlite3.OperationalError as exc:
    92:             if "locked" in str(exc).lower() and attempt < retries:
```

#### Line 98
```python
    96:     
    97:     # First create the main table if it doesn't exist
    98:     cursor.execute('''
    99:     CREATE TABLE IF NOT EXISTS trades (
   100:         id INTEGER PRIMARY KEY AUTOINCREMENT,
   101:         trade_date DATE NOT NULL,
   102:         value_date DATE NOT NULL,
```

#### Line 185
```python
   183:     
   184:     # Get existing columns
   185:     cursor.execute("PRAGMA table_info(trades)")
   186:     existing_columns = [col[1] for col in cursor.fetchall()]
   187:     
   188:     # Add missing columns
   189:     for column_name, column_type in columns_to_add:
```

#### Line 191
```python
   189:     for column_name, column_type in columns_to_add:
   190:         if column_name not in existing_columns:
   191:             cursor.execute(f"ALTER TABLE trades ADD COLUMN {column_name} {column_type}")
   192:     
   193:     # Create indexes for faster queries
   194:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
   195:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
```

#### Line 194
```python
   192:     
   193:     # Create indexes for faster queries
   194:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
   195:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
   196:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
   197:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
   198:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')
```

#### Line 195
```python
   193:     # Create indexes for faster queries
   194:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
   195:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
   196:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
   197:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
   198:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')
   199: 
```

#### Line 196
```python
   194:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
   195:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
   196:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
   197:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
   198:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')
   199: 
   200:     # Backfill support doc from OrderID when available
```

#### Line 197
```python
   195:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_date ON trades(trade_date)')
   196:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
   197:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
   198:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')
   199: 
   200:     # Backfill support doc from OrderID when available
   201:     if 'fnc_number' in existing_columns or 'fnc_number' in [c[0] for c in columns_to_add]:
```

#### Line 198
```python
   196:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_id ON trades(order_id)')
   197:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_fix_trade_id ON trades(fix_trade_id)')
   198:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_rest_trade_id ON trades(rest_trade_id)')
   199: 
   200:     # Backfill support doc from OrderID when available
   201:     if 'fnc_number' in existing_columns or 'fnc_number' in [c[0] for c in columns_to_add]:
   202:         cursor.execute("""
```

#### Line 202
```python
   200:     # Backfill support doc from OrderID when available
   201:     if 'fnc_number' in existing_columns or 'fnc_number' in [c[0] for c in columns_to_add]:
   202:         cursor.execute("""
   203:             UPDATE trades
   204:             SET fnc_number = order_id
   205:             WHERE (fnc_number IS NULL OR fnc_number = '')
   206:               AND (
```

#### Line 213
```python
   211:         """)
   212:     
   213:     commit_with_mirror(conn)
   214:     conn.close()
   215: 
```

### models\trade.py

#### Line 52
```python
    50: def load_all_trades():
    51:     """Load all trades from database - handles missing FIX columns"""
    52:     conn = get_db_connection()
    53:     try:
    54:         # First check which columns exist
    55:         cursor = conn.cursor()
    56:         cursor.execute("PRAGMA table_info(trades)")
```

#### Line 56
```python
    54:         # First check which columns exist
    55:         cursor = conn.cursor()
    56:         cursor.execute("PRAGMA table_info(trades)")
    57:         columns_info = cursor.fetchall()
    58:         existing_columns = [col[1] for col in columns_info]
    59:         
    60:         # Build query based on available columns
```

#### Line 125
```python
   123:         """
   124:         
   125:         df = pd.read_sql(query, conn)
   126:         
   127:         # Convert date columns
   128:         date_columns = ['Trade Date', 'Value Date', 'Created At']
   129:         for col in date_columns:
```

#### Line 148
```python
   146:         str in YYYYMMDD format, or None when no date is available.
   147:     """
   148:     conn = get_db_connection()
   149:     try:
   150:         cursor = conn.cursor()
   151:         if source_system:
   152:             cursor.execute(
```

#### Line 152
```python
   150:         cursor = conn.cursor()
   151:         if source_system:
   152:             cursor.execute(
   153:                 """
   154:                 SELECT MAX(trade_date)
   155:                 FROM trades
   156:                 WHERE source_system = ?
```

#### Line 163
```python
   161:             )
   162:         else:
   163:             cursor.execute(
   164:                 """
   165:                 SELECT MAX(trade_date)
   166:                 FROM trades
   167:                 WHERE trade_date IS NOT NULL
```

#### Line 195
```python
   193:         int or None when no numeric doc_number is available.
   194:     """
   195:     conn = get_db_connection()
   196:     try:
   197:         cursor = conn.cursor()
   198:         base_query = """
   199:             SELECT MAX(CAST(TRIM(doc_number) AS INTEGER))
```

#### Line 206
```python
   204:         """
   205:         if source_system:
   206:             cursor.execute(base_query + " AND source_system = ?", (source_system,))
   207:         else:
   208:             cursor.execute(base_query)
   209:         row = cursor.fetchone()
   210:         latest = row[0] if row else None
```

#### Line 208
```python
   206:             cursor.execute(base_query + " AND source_system = ?", (source_system,))
   207:         else:
   208:             cursor.execute(base_query)
   209:         row = cursor.fetchone()
   210:         latest = row[0] if row else None
   211:         return int(latest) if latest is not None else None
   212:     except Exception:
```

#### Line 228
```python
   226:         }
   227:     """
   228:     conn = get_db_connection()
   229:     try:
   230:         cursor = conn.cursor()
   231:         if source_system:
   232:             cursor.execute(
```

#### Line 232
```python
   230:         cursor = conn.cursor()
   231:         if source_system:
   232:             cursor.execute(
   233:                 """
   234:                 SELECT MIN(trade_date), MAX(trade_date)
   235:                 FROM trades
   236:                 WHERE source_system = ?
```

#### Line 243
```python
   241:             )
   242:         else:
   243:             cursor.execute(
   244:                 """
   245:                 SELECT MIN(trade_date), MAX(trade_date)
   246:                 FROM trades
   247:                 WHERE trade_date IS NOT NULL
```

#### Line 272
```python
   270: def count_missing_support_docs(source_system: Optional[str] = "Murex") -> Dict[str, int]:
   271:     """Count trades with/without supporting doc values (FNC/SWT token)."""
   272:     conn = get_db_connection()
   273:     try:
   274:         cursor = conn.cursor()
   275:         if source_system:
   276:             cursor.execute(
```

#### Line 276
```python
   274:         cursor = conn.cursor()
   275:         if source_system:
   276:             cursor.execute(
   277:                 """
   278:                 SELECT COUNT(*)
   279:                 FROM trades
   280:                 WHERE source_system = ?
```

#### Line 285
```python
   283:             )
   284:             total = int(cursor.fetchone()[0] or 0)
   285:             cursor.execute(
   286:                 """
   287:                 SELECT COUNT(*)
   288:                 FROM trades
   289:                 WHERE source_system = ?
```

#### Line 295
```python
   293:             )
   294:         else:
   295:             cursor.execute("SELECT COUNT(*) FROM trades")
   296:             total = int(cursor.fetchone()[0] or 0)
   297:             cursor.execute(
   298:                 """
   299:                 SELECT COUNT(*)
```

#### Line 297
```python
   295:             cursor.execute("SELECT COUNT(*) FROM trades")
   296:             total = int(cursor.fetchone()[0] or 0)
   297:             cursor.execute(
   298:                 """
   299:                 SELECT COUNT(*)
   300:                 FROM trades
   301:                 WHERE fnc_number IS NULL OR TRIM(fnc_number) = ''
```

#### Line 465
```python
   463:         sig_map.pop(k, None)
   464: 
   465:     conn = get_db_connection()
   466:     cursor = conn.cursor()
   467:     try:
   468:         where_clauses = ["1=1"]
   469:         params: List = []
```

#### Line 481
```python
   479:             WHERE {' AND '.join(where_clauses)}
   480:         """
   481:         cursor.execute(query, params)
   482:         rows = cursor.fetchall()
   483: 
   484:         updates_by_id: Dict[int, str] = {}
   485:         matched_by_doc = 0
```

#### Line 529
```python
   527:         updates = [(doc, trade_id) for trade_id, doc in updates_by_id.items()]
   528:         if updates:
   529:             cursor.executemany(
   530:                 """
   531:                 UPDATE trades
   532:                 SET fnc_number = ?
   533:                 WHERE id = ?
```

#### Line 539
```python
   537: 
   538:         # Also backfill from order_id when manually keyed as FNC/SWT.
   539:         cursor.execute(
   540:             """
   541:             UPDATE trades
   542:             SET fnc_number = order_id
   543:             WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
```

#### Line 557
```python
   555:             missing_where.append("source_system = ?")
   556:             missing_params.append(source_system)
   557:         cursor.execute(
   558:             f"SELECT COUNT(*) FROM trades WHERE {' AND '.join(missing_where)}",
   559:             missing_params,
   560:         )
   561:         unresolved_after = int(cursor.fetchone()[0] or 0)
```

#### Line 563
```python
   561:         unresolved_after = int(cursor.fetchone()[0] or 0)
   562: 
   563:         commit_with_mirror(conn)
   564:         return {
   565:             "history_rows": int(len(history_df)),
   566:             "history_with_support_doc": int(history_with_support_doc),
   567:             "candidate_rows": int(len(rows)),
```

#### Line 581
```python
   579: def add_fix_trade(trade_data: Dict) -> bool:
   580:     """Add a trade from FIX API to database with balance calculation"""
   581:     conn = get_db_connection()
   582:     cursor = conn.cursor()
   583:     
   584:     try:
   585:         # Set debit/credit based on symbol and side
```

#### Line 616
```python
   614:         
   615:         # Get latest balances
   616:         cursor.execute("""
   617:             SELECT balance_usd, balance_zar, balance_xau 
   618:             FROM trades 
   619:             ORDER BY trade_date DESC, created_at DESC 
   620:             LIMIT 1
```

#### Line 635
```python
   633:         
   634:         # Insert into database
   635:         cursor.execute('''
   636:         INSERT INTO trades (
   637:             trade_date, value_date, symbol, side, narration, quantity, price,
   638:             settle_currency, settle_amount, doc_number, clord_id, order_id,
   639:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 659
```python
   657:         ))
   658:         
   659:         commit_with_mirror(conn)
   660:         return True
   661:         
   662:     except sqlite3.IntegrityError as e:
   663:         conn.rollback()
```

#### Line 663
```python
   661:         
   662:     except sqlite3.IntegrityError as e:
   663:         conn.rollback()
   664:         # Handle duplicate doc_number error
   665:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   666:             # Generate a new unique doc_number
   667:             import time
```

#### Line 675
```python
   673:             raise Exception(f"Database integrity error: {e}")
   674:     except Exception as e:
   675:         conn.rollback()
   676:         raise Exception(f"Error adding FIX trade: {e}")
   677:     finally:
   678:         conn.close()
   679: 
```

#### Line 683
```python
   681: def add_new_trade(trade_data):
   682:     """Add a new trade manually (from sidebar form)"""
   683:     conn = get_db_connection()
   684:     cursor = conn.cursor()
   685: 
   686:     try:
   687:         cursor.execute("""
```

#### Line 687
```python
   685: 
   686:     try:
   687:         cursor.execute("""
   688:             SELECT balance_usd, balance_zar, balance_xau 
   689:             FROM trades 
   690:             ORDER BY trade_date DESC, created_at DESC 
   691:             LIMIT 1
```

#### Line 704
```python
   702:             balance_xau = trade_data['credit_xau'] - trade_data['debit_xau']
   703: 
   704:         cursor.execute('''
   705:         INSERT INTO trades (
   706:             trade_date, value_date, symbol, side, narration, quantity, price,
   707:             settle_currency, settle_amount, doc_number, clord_id, order_id,
   708:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 722
```python
   720:         ))
   721: 
   722:         commit_with_mirror(conn)
   723:         return True
   724:     except sqlite3.IntegrityError as e:
   725:         conn.rollback()
   726:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
```

#### Line 725
```python
   723:         return True
   724:     except sqlite3.IntegrityError as e:
   725:         conn.rollback()
   726:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   727:             raise Exception(f"Document number '{trade_data['doc_number']}' already exists. Please use a unique document number.")
   728:         else:
   729:             raise Exception(f"Database integrity error: {e}")
```

#### Line 731
```python
   729:             raise Exception(f"Database integrity error: {e}")
   730:     except Exception as e:
   731:         conn.rollback()
   732:         raise Exception(f"Error adding trade: {e}")
   733:     finally:
   734:         conn.close()
   735: 
```

#### Line 739
```python
   737: def update_trade_order_id(trade_id: int, order_id: str) -> bool:
   738:     """Update the order_id (MetCon Trade Number) for a specific trade"""
   739:     conn = get_db_connection()
   740:     cursor = conn.cursor()
   741:     
   742:     try:
   743:         # Convert empty string to None for database
```

#### Line 746
```python
   744:         order_id_value = order_id.strip().upper() if order_id and order_id.strip() else None
   745:         
   746:         cursor.execute('''
   747:         UPDATE trades 
   748:         SET order_id = ?
   749:         WHERE id = ?
   750:         ''', (order_id_value, trade_id))
```

#### Line 752
```python
   750:         ''', (order_id_value, trade_id))
   751:         if cursor.rowcount == 0:
   752:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
   753:             if cursor.fetchone() is None:
   754:                 conn.rollback()
   755:                 return False
   756: 
```

#### Line 754
```python
   752:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
   753:             if cursor.fetchone() is None:
   754:                 conn.rollback()
   755:                 return False
   756: 
   757:         # If a single supporting doc exists for this Trade #, propagate it to blank rows.
   758:         if order_id_value:
```

#### Line 759
```python
   757:         # If a single supporting doc exists for this Trade #, propagate it to blank rows.
   758:         if order_id_value:
   759:             cursor.execute(
   760:                 """
   761:                 SELECT DISTINCT fnc_number
   762:                 FROM trades
   763:                 WHERE order_id = ?
```

#### Line 771
```python
   769:             docs = [row[0] for row in cursor.fetchall() if row and row[0]]
   770:             if len(docs) == 1:
   771:                 cursor.execute(
   772:                     """
   773:                     UPDATE trades
   774:                     SET fnc_number = ?
   775:                     WHERE order_id = ?
```

#### Line 781
```python
   779:                 )
   780: 
   781:         commit_with_mirror(conn)
   782: 
   783:         # Keep JSON backup in sync so manual assignments survive replacement flows.
   784:         try:
   785:             backup_manual_trades_to_json()
```

#### Line 791
```python
   789:         return True
   790:     except Exception as e:
   791:         conn.rollback()
   792:         raise Exception(f"Error updating trade number: {e}")
   793:     finally:
   794:         conn.close()
   795: 
```

#### Line 803
```python
   801:     Normalizes the trade data from StoneX API format and calculates debit/credit.
   802:     """
   803:     conn = get_db_connection()
   804:     cursor = conn.cursor()
   805:     
   806:     try:
   807:         # Extract key fields from REST API response
```

#### Line 896
```python
   894:         
   895:         # Get latest balances
   896:         cursor.execute("""
   897:             SELECT balance_usd, balance_zar, balance_xau 
   898:             FROM trades 
   899:             ORDER BY trade_date DESC, created_at DESC 
   900:             LIMIT 1
```

#### Line 914
```python
   912:         
   913:         # Insert into database with all REST API fields
   914:         cursor.execute('''
   915:         INSERT INTO trades (
   916:             trade_date, value_date, symbol, side, narration, quantity, price,
   917:             settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
   918:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 953
```python
   951:         ))
   952:         
   953:         commit_with_mirror(conn)
   954:         return True
   955:         
   956:     except sqlite3.IntegrityError as e:
   957:         conn.rollback()
```

#### Line 957
```python
   955:         
   956:     except sqlite3.IntegrityError as e:
   957:         conn.rollback()
   958:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   959:             # Trade already exists, skip
   960:             return False
   961:         else:
```

#### Line 964
```python
   962:             raise Exception(f"Database integrity error: {e}")
   963:     except Exception as e:
   964:         conn.rollback()
   965:         raise Exception(f"Error adding REST trade: {e}")
   966:     finally:
   967:         conn.close()
   968: 
```

#### Line 979
```python
   977:         return {"inserted": 0, "skipped": 0}
   978: 
   979:     conn = get_db_connection()
   980:     cursor = conn.cursor()
   981: 
   982:     def first_non_empty(*vals):
   983:         for v in vals:
```

#### Line 1021
```python
  1019:     # Guard against duplicates where the same doc appears with formatting variants
  1020:     # like "45861303" vs "45861303.0".
  1021:     cursor.execute("SELECT doc_number FROM trades WHERE doc_number IS NOT NULL AND TRIM(doc_number) != ''")
  1022:     existing_docs = {
  1023:         _normalize_trade_key(row[0])
  1024:         for row in cursor.fetchall()
  1025:         if _normalize_trade_key(row[0])
```

#### Line 1104
```python
  1102: 
  1103:         try:
  1104:             cursor.execute(
  1105:                 '''
  1106:                 INSERT OR IGNORE INTO trades (
  1107:                     trade_date, value_date, symbol, side, narration, quantity, price,
  1108:                     settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
```

#### Line 1154
```python
  1152:     # Backfill known supporting docs for existing rows skipped due duplicate doc_number.
  1153:     if doc_support_updates:
  1154:         cursor.executemany(
  1155:             """
  1156:             UPDATE trades
  1157:             SET fnc_number = ?
  1158:             WHERE doc_number = ?
```

#### Line 1165
```python
  1163: 
  1164:     # Backfill supporting doc from OrderID when it carries FNC/SWT tokens.
  1165:     cursor.execute("""
  1166:         UPDATE trades
  1167:         SET fnc_number = order_id
  1168:         WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
  1169:           AND (
```

#### Line 1175
```python
  1173:           )
  1174:     """)
  1175:     commit_with_mirror(conn)
  1176:     conn.close()
  1177: 
  1178:     restored = 0
  1179:     try:
```

#### Line 1197
```python
  1195:         return {"inserted": 0, "skipped": 0}
  1196: 
  1197:     conn = get_db_connection()
  1198:     cursor = conn.cursor()
  1199: 
  1200:     def first_non_empty(*vals):
  1201:         for v in vals:
```

#### Line 1234
```python
  1232: 
  1233:     # 1. Backup existing manual overrides from Database
  1234:     cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
  1235:     preserved_ids_db = cursor.fetchall()
  1236:     print(f"[DEBUG] replace_trades_with_murex: Preserved {len(preserved_ids_db)} manual trade IDs from DB")
  1237:     if preserved_ids_db:
  1238:         print(f"[DEBUG] Sample preserved DB: {preserved_ids_db[:3]}")
```

#### Line 1269
```python
  1267: 
  1268:     # 2. Clear existing trades
  1269:     cursor.execute("DELETE FROM trades")
  1270: 
  1271:     inserted = 0
  1272:     skipped = 0
  1273:     seen_docs = set()
```

#### Line 1350
```python
  1348: 
  1349:         try:
  1350:             cursor.execute(
  1351:                 '''
  1352:                 INSERT INTO trades (
  1353:                     trade_date, value_date, symbol, side, narration, quantity, price,
  1354:                     settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
```

#### Line 1395
```python
  1393: 
  1394:     # Backfill supporting doc from OrderID when it carries FNC/SWT tokens.
  1395:     cursor.execute("""
  1396:         UPDATE trades
  1397:         SET fnc_number = order_id
  1398:         WHERE (fnc_number IS NULL OR TRIM(fnc_number) = '')
  1399:           AND (
```

#### Line 1408
```python
  1406:     # 3. Restore manual overrides (Trade Numbers)
  1407:     if preserved_ids:
  1408:         cursor.execute("SELECT id, doc_number FROM trades")
  1409:         inserted_rows = cursor.fetchall()
  1410:         trade_ids_by_doc = {}
  1411:         for trade_id, raw_doc in inserted_rows:
  1412:             doc_key = _normalize_trade_key(raw_doc)
```

#### Line 1426
```python
  1424: 
  1425:         if restore_updates:
  1426:             cursor.executemany("UPDATE trades SET order_id = ? WHERE id = ?", restore_updates)
  1427:             print(f"[DEBUG] replace_trades_with_murex: Restored {len(restore_updates)} manual trade IDs")
  1428: 
  1429:     commit_with_mirror(conn)
  1430:     conn.close()
```

#### Line 1429
```python
  1427:             print(f"[DEBUG] replace_trades_with_murex: Restored {len(restore_updates)} manual trade IDs")
  1428: 
  1429:     commit_with_mirror(conn)
  1430:     conn.close()
  1431: 
  1432:     restored = 0
  1433:     try:
```

#### Line 1447
```python
  1445:         return {"restored": 0, "available": 0}
  1446: 
  1447:     conn = get_db_connection()
  1448:     cursor = conn.cursor()
  1449:     try:
  1450:         with open(MANUAL_TRADES_FILE, "r") as f:
  1451:             data = json.load(f)
```

#### Line 1466
```python
  1464:             return {"restored": 0, "available": 0}
  1465: 
  1466:         cursor.execute("SELECT id, doc_number, order_id FROM trades")
  1467:         db_rows = cursor.fetchall()
  1468:         trade_ids_by_doc = {}
  1469:         for trade_id, raw_doc, raw_order in db_rows:
  1470:             doc_key = _normalize_trade_key(raw_doc)
```

#### Line 1490
```python
  1488: 
  1489:         before = conn.total_changes
  1490:         cursor.executemany(
  1491:             "UPDATE trades SET order_id = ? WHERE id = ?",
  1492:             id_updates,
  1493:         )
  1494: 
```

#### Line 1495
```python
  1493:         )
  1494: 
  1495:         commit_with_mirror(conn)
  1496:         restored = conn.total_changes - before
  1497:         return {"restored": restored, "available": len(updates)}
  1498:     except Exception as e:
  1499:         print(f"[ERROR] Failed to restore manual trades: {e}")
```

#### Line 1507
```python
  1505: def backup_manual_trades_to_json() -> bool:
  1506:     """Save all manual trade assignments to a JSON file for persistence."""
  1507:     conn = get_db_connection()
  1508:     cursor = conn.cursor()
  1509:     try:
  1510:         cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
  1511:         rows = cursor.fetchall()
```

#### Line 1510
```python
  1508:     cursor = conn.cursor()
  1509:     try:
  1510:         cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
  1511:         rows = cursor.fetchall()
  1512:         
  1513:         data = {}
  1514:         for doc_number, order_id in rows:
```

### models\trade-MCSEZNBJKRESS.py

#### Line 34
```python
    32: def load_all_trades():
    33:     """Load all trades from database - handles missing FIX columns"""
    34:     conn = get_db_connection()
    35:     try:
    36:         # First check which columns exist
    37:         cursor = conn.cursor()
    38:         cursor.execute("PRAGMA table_info(trades)")
```

#### Line 38
```python
    36:         # First check which columns exist
    37:         cursor = conn.cursor()
    38:         cursor.execute("PRAGMA table_info(trades)")
    39:         columns_info = cursor.fetchall()
    40:         existing_columns = [col[1] for col in columns_info]
    41:         
    42:         # Build query based on available columns
```

#### Line 107
```python
   105:         """
   106:         
   107:         df = pd.read_sql(query, conn)
   108:         
   109:         # Convert date columns
   110:         date_columns = ['Trade Date', 'Value Date', 'Created At']
   111:         for col in date_columns:
```

#### Line 125
```python
   123: def add_fix_trade(trade_data: Dict) -> bool:
   124:     """Add a trade from FIX API to database with balance calculation"""
   125:     conn = get_db_connection()
   126:     cursor = conn.cursor()
   127:     
   128:     try:
   129:         # Set debit/credit based on symbol and side
```

#### Line 160
```python
   158:         
   159:         # Get latest balances
   160:         cursor.execute("""
   161:             SELECT balance_usd, balance_zar, balance_xau 
   162:             FROM trades 
   163:             ORDER BY trade_date DESC, created_at DESC 
   164:             LIMIT 1
```

#### Line 179
```python
   177:         
   178:         # Insert into database
   179:         cursor.execute('''
   180:         INSERT INTO trades (
   181:             trade_date, value_date, symbol, side, narration, quantity, price,
   182:             settle_currency, settle_amount, doc_number, clord_id, order_id,
   183:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 207
```python
   205:         
   206:     except sqlite3.IntegrityError as e:
   207:         conn.rollback()
   208:         # Handle duplicate doc_number error
   209:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   210:             # Generate a new unique doc_number
   211:             import time
```

#### Line 219
```python
   217:             raise Exception(f"Database integrity error: {e}")
   218:     except Exception as e:
   219:         conn.rollback()
   220:         raise Exception(f"Error adding FIX trade: {e}")
   221:     finally:
   222:         conn.close()
   223: 
```

#### Line 227
```python
   225: def add_new_trade(trade_data):
   226:     """Add a new trade manually (from sidebar form)"""
   227:     conn = get_db_connection()
   228:     cursor = conn.cursor()
   229: 
   230:     try:
   231:         cursor.execute("""
```

#### Line 231
```python
   229: 
   230:     try:
   231:         cursor.execute("""
   232:             SELECT balance_usd, balance_zar, balance_xau 
   233:             FROM trades 
   234:             ORDER BY trade_date DESC, created_at DESC 
   235:             LIMIT 1
```

#### Line 248
```python
   246:             balance_xau = trade_data['credit_xau'] - trade_data['debit_xau']
   247: 
   248:         cursor.execute('''
   249:         INSERT INTO trades (
   250:             trade_date, value_date, symbol, side, narration, quantity, price,
   251:             settle_currency, settle_amount, doc_number, clord_id, order_id,
   252:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 269
```python
   267:         return True
   268:     except sqlite3.IntegrityError as e:
   269:         conn.rollback()
   270:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   271:             raise Exception(f"Document number '{trade_data['doc_number']}' already exists. Please use a unique document number.")
   272:         else:
   273:             raise Exception(f"Database integrity error: {e}")
```

#### Line 275
```python
   273:             raise Exception(f"Database integrity error: {e}")
   274:     except Exception as e:
   275:         conn.rollback()
   276:         raise Exception(f"Error adding trade: {e}")
   277:     finally:
   278:         conn.close()
   279: 
```

#### Line 283
```python
   281: def update_trade_order_id(trade_id: int, order_id: str) -> bool:
   282:     """Update the order_id (MetCon Trade Number) for a specific trade"""
   283:     conn = get_db_connection()
   284:     cursor = conn.cursor()
   285:     
   286:     try:
   287:         # Convert empty string to None for database
```

#### Line 290
```python
   288:         order_id_value = order_id.strip() if order_id and order_id.strip() else None
   289:         
   290:         cursor.execute('''
   291:         UPDATE trades 
   292:         SET order_id = ?
   293:         WHERE id = ?
   294:         ''', (order_id_value, trade_id))
```

#### Line 296
```python
   294:         ''', (order_id_value, trade_id))
   295:         if cursor.rowcount == 0:
   296:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
   297:             if cursor.fetchone() is None:
   298:                 conn.rollback()
   299:                 return False
   300: 
```

#### Line 298
```python
   296:             cursor.execute("SELECT 1 FROM trades WHERE id = ?", (trade_id,))
   297:             if cursor.fetchone() is None:
   298:                 conn.rollback()
   299:                 return False
   300: 
   301:         conn.commit()
   302: 
```

#### Line 311
```python
   309:         return True
   310:     except Exception as e:
   311:         conn.rollback()
   312:         raise Exception(f"Error updating trade number: {e}")
   313:     finally:
   314:         conn.close()
   315: 
```

#### Line 323
```python
   321:     Normalizes the trade data from StoneX API format and calculates debit/credit.
   322:     """
   323:     conn = get_db_connection()
   324:     cursor = conn.cursor()
   325:     
   326:     try:
   327:         # Extract key fields from REST API response
```

#### Line 405
```python
   403:         
   404:         # Get latest balances
   405:         cursor.execute("""
   406:             SELECT balance_usd, balance_zar, balance_xau 
   407:             FROM trades 
   408:             ORDER BY trade_date DESC, created_at DESC 
   409:             LIMIT 1
```

#### Line 423
```python
   421:         
   422:         # Insert into database with all REST API fields
   423:         cursor.execute('''
   424:         INSERT INTO trades (
   425:             trade_date, value_date, symbol, side, narration, quantity, price,
   426:             settle_currency, settle_amount, doc_number, clord_id, order_id,
   427:             debit_usd, credit_usd, debit_zar, credit_zar, debit_xau, credit_xau,
```

#### Line 466
```python
   464:         
   465:     except sqlite3.IntegrityError as e:
   466:         conn.rollback()
   467:         if "UNIQUE constraint failed: trades.doc_number" in str(e):
   468:             # Trade already exists, skip
   469:             return False
   470:         else:
```

#### Line 473
```python
   471:             raise Exception(f"Database integrity error: {e}")
   472:     except Exception as e:
   473:         conn.rollback()
   474:         raise Exception(f"Error adding REST trade: {e}")
   475:     finally:
   476:         conn.close()
   477: 
```

#### Line 488
```python
   486:         return {"inserted": 0, "skipped": 0}
   487: 
   488:     conn = get_db_connection()
   489:     cursor = conn.cursor()
   490: 
   491:     def first_non_empty(*vals):
   492:         for v in vals:
```

#### Line 611
```python
   609: 
   610:         try:
   611:             cursor.execute(
   612:                 '''
   613:                 INSERT OR IGNORE INTO trades (
   614:                     trade_date, value_date, symbol, side, narration, quantity, price,
   615:                     settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
```

#### Line 659
```python
   657: 
   658:     # Backfill FNC number from OrderID if missing
   659:     cursor.execute("""
   660:         UPDATE trades
   661:         SET fnc_number = order_id
   662:         WHERE (fnc_number IS NULL OR fnc_number = '')
   663:           AND order_id LIKE 'FNC/%'
```

#### Line 666
```python
   664:     """)
   665:     # Backfill FNC number from OrderID if missing
   666:     cursor.execute("""
   667:         UPDATE trades
   668:         SET fnc_number = order_id
   669:         WHERE (fnc_number IS NULL OR fnc_number = '')
   670:           AND order_id LIKE 'FNC/%'
```

#### Line 694
```python
   692:         return {"inserted": 0, "skipped": 0}
   693: 
   694:     conn = get_db_connection()
   695:     cursor = conn.cursor()
   696: 
   697:     def first_non_empty(*vals):
   698:         for v in vals:
```

#### Line 734
```python
   732: 
   733:     # 1. Backup existing manual overrides from Database
   734:     cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
   735:     preserved_ids_db = cursor.fetchall()
   736:     print(f"[DEBUG] replace_trades_with_murex: Preserved {len(preserved_ids_db)} manual trade IDs from DB")
   737:     if preserved_ids_db:
   738:         print(f"[DEBUG] Sample preserved DB: {preserved_ids_db[:3]}")
```

#### Line 764
```python
   762: 
   763:     # 2. Clear existing trades
   764:     cursor.execute("DELETE FROM trades")
   765: 
   766:     inserted = 0
   767:     skipped = 0
   768:     seen_docs = set()
```

#### Line 845
```python
   843: 
   844:         try:
   845:             cursor.execute(
   846:                 '''
   847:                 INSERT INTO trades (
   848:                     trade_date, value_date, symbol, side, narration, quantity, price,
   849:                     settle_currency, settle_amount, doc_number, clord_id, order_id, fnc_number,
```

#### Line 891
```python
   889:     # 3. Restore manual overrides (Trade Numbers)
   890:     if preserved_ids:
   891:         cursor.executemany("UPDATE trades SET order_id = ? WHERE doc_number = ?", 
   892:                           [(oid, doc) for doc, oid in preserved_ids])
   893:         print(f"[DEBUG] replace_trades_with_murex: Restored {cursor.rowcount} manual trade IDs")
   894: 
   895:     conn.commit()
```

#### Line 910
```python
   908: def backup_manual_trades_to_json() -> bool:
   909:     """Save all manual trade assignments to a JSON file for persistence."""
   910:     conn = get_db_connection()
   911:     cursor = conn.cursor()
   912:     try:
   913:         cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
   914:         rows = cursor.fetchall()
```

#### Line 913
```python
   911:     cursor = conn.cursor()
   912:     try:
   913:         cursor.execute("SELECT doc_number, order_id FROM trades WHERE order_id IS NOT NULL AND order_id != ''")
   914:         rows = cursor.fetchall()
   915:         
   916:         data = {row[0]: row[1] for row in rows}
   917:         
```

### services\clean_data_pipeline.py

#### Line 19
```python
    17: 
    18: def _get_conn(path: str) -> sqlite3.Connection:
    19:     conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    20:     conn.row_factory = sqlite3.Row
    21:     conn.execute("PRAGMA journal_mode=WAL;")
    22:     conn.execute("PRAGMA synchronous=NORMAL;")
    23:     conn.execute("PRAGMA foreign_keys=ON;")
```

#### Line 218
```python
   216:         return 0
   217:     cur = conn.cursor()
   218:     cur.executemany(upsert_sql, rows)
   219:     return len(rows)
   220: 
   221: 
   222: def run_clean_data_pipeline(
```

### services\trademc_service.py

#### Line 163
```python
   161: def _verify_full_replace_sync(expected_by_id: Dict[int, Dict[str, Any]], synced_at: str) -> Dict[str, Any]:
   162:     """Verify that local rows written in a full replace match the remote snapshot used for the write."""
   163:     conn = sqlite3.connect(DB_PATH)
   164:     conn.row_factory = sqlite3.Row
   165:     cursor = conn.cursor()
   166:     cursor.execute(
   167:         """
```

#### Line 166
```python
   164:     conn.row_factory = sqlite3.Row
   165:     cursor = conn.cursor()
   166:     cursor.execute(
   167:         """
   168:         SELECT id, status, company_id, weight, notes, ref_number,
   169:                trade_timestamp, zar_per_troy_ounce, zar_to_usd,
   170:                requested_zar_per_gram, zar_per_troy_ounce_confirmed,
```

#### Line 230
```python
   228:         return {}
   229: 
   230:     conn = sqlite3.connect(DB_PATH)
   231:     conn.row_factory = sqlite3.Row
   232:     cursor = conn.cursor()
   233:     out: Dict[int, Dict[str, Any]] = {}
   234:     chunk_size = 900
```

#### Line 241
```python
   239:             chunk = trade_ids[i:i + chunk_size]
   240:             placeholders = ",".join("?" for _ in chunk)
   241:             cursor.execute(
   242:                 f"SELECT {select_cols} FROM trademc_trades WHERE id IN ({placeholders})",
   243:                 chunk,
   244:             )
   245:             for row in cursor.fetchall():
```

#### Line 398
```python
   396: def initialize_trademc_table():
   397:     """Create the TradeMC tables if they don't exist."""
   398:     conn = sqlite3.connect(DB_PATH)
   399:     cursor = conn.cursor()
   400:     
   401:     # Create trades table
   402:     cursor.execute('''
```

#### Line 402
```python
   400:     
   401:     # Create trades table
   402:     cursor.execute('''
   403:         CREATE TABLE IF NOT EXISTS trademc_trades (
   404:             id INTEGER PRIMARY KEY,
   405:             status TEXT,
   406:             company_id INTEGER,
```

#### Line 425
```python
   423:     
   424:     # Create companies table
   425:     cursor.execute('''
   426:         CREATE TABLE IF NOT EXISTS trademc_companies (
   427:             id INTEGER PRIMARY KEY,
   428:             status TEXT,
   429:             company_name TEXT,
```

#### Line 446
```python
   444: 
   445:     # Create weight transaction ledger table
   446:     cursor.execute('''
   447:         CREATE TABLE IF NOT EXISTS trademc_weight_transactions (
   448:             id INTEGER PRIMARY KEY,
   449:             user_created TEXT,
   450:             date_created DATETIME,
```

#### Line 469
```python
   467:     
   468:     # Create indexes
   469:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_status ON trademc_trades(status)')
   470:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_notes ON trademc_trades(notes)')
   471:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
```

#### Line 470
```python
   468:     # Create indexes
   469:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_status ON trademc_trades(status)')
   470:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_notes ON trademc_trades(notes)')
   471:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
```

#### Line 471
```python
   469:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_status ON trademc_trades(status)')
   470:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_notes ON trademc_trades(notes)')
   471:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
```

#### Line 472
```python
   470:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_notes ON trademc_trades(notes)')
   471:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
```

#### Line 473
```python
   471:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
```

#### Line 474
```python
   472:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
   478:     
```

#### Line 475
```python
   473:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
   478:     
   479:     commit_with_mirror(conn, DB_PATH)
```

#### Line 476
```python
   474:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
   478:     
   479:     commit_with_mirror(conn, DB_PATH)
   480:     conn.close()
```

#### Line 477
```python
   475:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
   476:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
   478:     
   479:     commit_with_mirror(conn, DB_PATH)
   480:     conn.close()
   481: 
```

#### Line 479
```python
   477:     cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
   478:     
   479:     commit_with_mirror(conn, DB_PATH)
   480:     conn.close()
   481: 
   482: 
   483: 
```

#### Line 701
```python
   699: 
   700:     synced_at = datetime.now().isoformat()
   701:     conn = sqlite3.connect(DB_PATH)
   702:     cursor = conn.cursor()
   703:     cursor.execute(
   704:         """
   705:         INSERT INTO trademc_trades (
```

#### Line 703
```python
   701:     conn = sqlite3.connect(DB_PATH)
   702:     cursor = conn.cursor()
   703:     cursor.execute(
   704:         """
   705:         INSERT INTO trademc_trades (
   706:             id, status, company_id, weight, notes, ref_number,
   707:             trade_timestamp, zar_per_troy_ounce, zar_to_usd,
```

#### Line 750
```python
   748:         ),
   749:     )
   750:     commit_with_mirror(conn, DB_PATH)
   751:     conn.close()
   752: 
   753:     remote_ref = str(remote_trade.get("ref_number") or "")
   754:     return {
```

#### Line 767
```python
   765:     """Get the latest TradeMC trade ID stored in the local database."""
   766:     initialize_trademc_table()
   767:     conn = sqlite3.connect(DB_PATH)
   768:     cursor = conn.cursor()
   769:     cursor.execute("SELECT MAX(id) FROM trademc_trades")
   770:     latest = cursor.fetchone()[0]
   771:     conn.close()
```

#### Line 769
```python
   767:     conn = sqlite3.connect(DB_PATH)
   768:     cursor = conn.cursor()
   769:     cursor.execute("SELECT MAX(id) FROM trademc_trades")
   770:     latest = cursor.fetchone()[0]
   771:     conn.close()
   772:     return int(latest) if latest is not None else None
   773: 
```

#### Line 778
```python
   776:     """Get latest non-empty TradeMC date_updated from local cache."""
   777:     initialize_trademc_table()
   778:     conn = sqlite3.connect(DB_PATH)
   779:     cursor = conn.cursor()
   780:     cursor.execute(
   781:         """
   782:         SELECT MAX(date_updated)
```

#### Line 780
```python
   778:     conn = sqlite3.connect(DB_PATH)
   779:     cursor = conn.cursor()
   780:     cursor.execute(
   781:         """
   782:         SELECT MAX(date_updated)
   783:         FROM trademc_trades
   784:         WHERE date_updated IS NOT NULL AND TRIM(date_updated) <> ''
```

#### Line 796
```python
   794:     """Return local TradeMC cache stats for diagnostics/safety checks."""
   795:     initialize_trademc_table()
   796:     conn = sqlite3.connect(DB_PATH)
   797:     cursor = conn.cursor()
   798:     cursor.execute(
   799:         """
   800:         SELECT COUNT(*), MAX(id), MAX(date_updated)
```

#### Line 798
```python
   796:     conn = sqlite3.connect(DB_PATH)
   797:     cursor = conn.cursor()
   798:     cursor.execute(
   799:         """
   800:         SELECT COUNT(*), MAX(id), MAX(date_updated)
   801:         FROM trademc_trades
   802:         """
```

#### Line 1238
```python
  1236: 
  1237:     # Insert/update trades in database
  1238:     conn = sqlite3.connect(DB_PATH)
  1239:     cursor = conn.cursor()
  1240: 
  1241:     synced_at = datetime.now().isoformat()
  1242:     unique_by_id: Dict[int, Dict[str, Any]] = {}
```

#### Line 1292
```python
  1290:         }
  1291:         local_count_before = int(local_snapshot_before.get("count") or 0)
  1292:         cursor.execute("DELETE FROM trademc_trades")
  1293:         removed = int(
  1294:             cursor.rowcount
  1295:             if cursor.rowcount is not None and int(cursor.rowcount) >= 0
  1296:             else local_count_before
```

#### Line 1300
```python
  1298: 
  1299:         if trade_rows:
  1300:             cursor.executemany('''
  1301:                 INSERT INTO trademc_trades (
  1302:                     id, status, company_id, weight, notes, ref_number,
  1303:                     trade_timestamp, zar_per_troy_ounce, zar_to_usd,
  1304:                     requested_zar_per_gram, zar_per_troy_ounce_confirmed,
```

#### Line 1310
```python
  1308:             ''', trade_rows)
  1309: 
  1310:         commit_with_mirror(conn, DB_PATH)
  1311:         conn.close()
  1312:         verification = _verify_full_replace_sync(expected_by_id=expected_by_id, synced_at=synced_at)
  1313:         if not bool(verification.get("ok")):
  1314:             verification_error = (
```

#### Line 1394
```python
  1392:         inserted = sum(1 for trade_id in ids_to_upsert if trade_id not in existing_ids)
  1393:         updated = len(ids_to_upsert) - inserted
  1394:         cursor.executemany('''
  1395:             INSERT INTO trademc_trades (
  1396:                 id, status, company_id, weight, notes, ref_number,
  1397:                 trade_timestamp, zar_per_troy_ounce, zar_to_usd,
  1398:                 requested_zar_per_gram, zar_per_troy_ounce_confirmed,
```

#### Line 1445
```python
  1443:             )
  1444:         ):
  1445:             cursor.execute(
  1446:                 """
  1447:                 SELECT COUNT(*)
  1448:                 FROM trademc_trades
  1449:                 WHERE id > ?
```

#### Line 1455
```python
  1453:             tail_remove = int(cursor.fetchone()[0] or 0)
  1454:             if tail_remove > 0:
  1455:                 cursor.execute(
  1456:                     """
  1457:                     DELETE FROM trademc_trades
  1458:                     WHERE id > ?
  1459:                     """,
```

#### Line 1489
```python
  1487: 
  1488:         if safe_to_prune and remote_trade_ids:
  1489:             cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _trademc_sync_remote_ids (id INTEGER PRIMARY KEY)")
  1490:             cursor.execute("DELETE FROM _trademc_sync_remote_ids")
  1491:             cursor.executemany(
  1492:                 "INSERT OR IGNORE INTO _trademc_sync_remote_ids (id) VALUES (?)",
  1493:                 [(trade_id,) for trade_id in remote_trade_ids],
```

#### Line 1490
```python
  1488:         if safe_to_prune and remote_trade_ids:
  1489:             cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _trademc_sync_remote_ids (id INTEGER PRIMARY KEY)")
  1490:             cursor.execute("DELETE FROM _trademc_sync_remote_ids")
  1491:             cursor.executemany(
  1492:                 "INSERT OR IGNORE INTO _trademc_sync_remote_ids (id) VALUES (?)",
  1493:                 [(trade_id,) for trade_id in remote_trade_ids],
  1494:             )
```

#### Line 1491
```python
  1489:             cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _trademc_sync_remote_ids (id INTEGER PRIMARY KEY)")
  1490:             cursor.execute("DELETE FROM _trademc_sync_remote_ids")
  1491:             cursor.executemany(
  1492:                 "INSERT OR IGNORE INTO _trademc_sync_remote_ids (id) VALUES (?)",
  1493:                 [(trade_id,) for trade_id in remote_trade_ids],
  1494:             )
  1495:             cursor.execute(
```

#### Line 1495
```python
  1493:                 [(trade_id,) for trade_id in remote_trade_ids],
  1494:             )
  1495:             cursor.execute(
  1496:                 """
  1497:                 SELECT COUNT(*)
  1498:                 FROM trademc_trades
  1499:                 WHERE id NOT IN (SELECT id FROM _trademc_sync_remote_ids)
```

#### Line 1503
```python
  1501:             )
  1502:             removed = int(cursor.fetchone()[0] or 0)
  1503:             cursor.execute(
  1504:                 """
  1505:                 DELETE FROM trademc_trades
  1506:                 WHERE id NOT IN (SELECT id FROM _trademc_sync_remote_ids)
  1507:                 """
```

#### Line 1513
```python
  1511: 
  1512:     if not trade_rows and not (prune_missing and not incremental):
  1513:         commit_with_mirror(conn, DB_PATH)
  1514:         conn.close()
  1515:         return {
  1516:             "success": True,
  1517:             "count": 0,
```

#### Line 1545
```python
  1543:         }
  1544:     
  1545:     commit_with_mirror(conn, DB_PATH)
  1546:     conn.close()
  1547:     local_snapshot_after = get_local_trademc_snapshot_stats()
  1548:     
  1549:     return {
```

#### Line 1624
```python
  1622:     """Get the latest TradeMC weight transaction ID stored in the local database."""
  1623:     initialize_trademc_table()
  1624:     conn = sqlite3.connect(DB_PATH)
  1625:     cursor = conn.cursor()
  1626:     cursor.execute("SELECT MAX(id) FROM trademc_weight_transactions")
  1627:     latest = cursor.fetchone()[0]
  1628:     conn.close()
```

#### Line 1626
```python
  1624:     conn = sqlite3.connect(DB_PATH)
  1625:     cursor = conn.cursor()
  1626:     cursor.execute("SELECT MAX(id) FROM trademc_weight_transactions")
  1627:     latest = cursor.fetchone()[0]
  1628:     conn.close()
  1629:     return int(latest) if latest is not None else None
  1630: 
```

#### Line 1733
```python
  1731:         return {"success": False, "error": "Failed to fetch weight transactions from API", "count": 0}
  1732: 
  1733:     conn = sqlite3.connect(DB_PATH)
  1734:     cursor = conn.cursor()
  1735: 
  1736:     synced_at = datetime.now().isoformat()
  1737:     unique_by_id: Dict[int, Dict[str, Any]] = {}
```

#### Line 1784
```python
  1782:         chunk = row_ids[i:i + chunk_size]
  1783:         placeholders = ",".join("?" for _ in chunk)
  1784:         cursor.execute(f"SELECT id FROM trademc_weight_transactions WHERE id IN ({placeholders})", chunk)
  1785:         existing_ids.update(item[0] for item in cursor.fetchall())
  1786: 
  1787:     inserted = sum(1 for row_id in row_ids if row_id not in existing_ids)
  1788:     updated = len(row_values) - inserted
```

#### Line 1790
```python
  1788:     updated = len(row_values) - inserted
  1789: 
  1790:     cursor.executemany('''
  1791:         INSERT INTO trademc_weight_transactions (
  1792:             user_created, date_created, user_updated, date_updated,
  1793:             company_id, pc_code, notes, type, weight, rolling_balance,
  1794:             transaction_timestamp, gold_percentage, old_id, reversal_id,
```

#### Line 1816
```python
  1814:     ''', row_values)
  1815: 
  1816:     commit_with_mirror(conn, DB_PATH)
  1817:     conn.close()
  1818: 
  1819:     return {
  1820:         "success": True,
```

#### Line 1855
```python
  1853:     initialize_trademc_table()
  1854:     
  1855:     conn = sqlite3.connect(DB_PATH)
  1856:     
  1857:     query = "SELECT * FROM trademc_trades WHERE 1=1"
  1858:     params = []
  1859:     
```

#### Line 1886
```python
  1884:     query += " ORDER BY trade_timestamp DESC"
  1885:     
  1886:     df = pd.read_sql_query(query, conn, params=params)
  1887:     conn.close()
  1888:     
  1889:     # Convert timestamp columns
  1890:     for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
```

#### Line 1907
```python
  1905:         dict: Trade data or None if not found
  1906:     """
  1907:     conn = sqlite3.connect(DB_PATH)
  1908:     cursor = conn.cursor()
  1909:     
  1910:     cursor.execute('SELECT * FROM trademc_trades WHERE id = ?', (trade_id,))
  1911:     row = cursor.fetchone()
```

#### Line 1910
```python
  1908:     cursor = conn.cursor()
  1909:     
  1910:     cursor.execute('SELECT * FROM trademc_trades WHERE id = ?', (trade_id,))
  1911:     row = cursor.fetchone()
  1912:     
  1913:     if row:
  1914:         columns = [description[0] for description in cursor.description]
```

#### Line 1930
```python
  1928:         dict: Statistics including count, date range, total weight, etc.
  1929:     """
  1930:     conn = sqlite3.connect(DB_PATH)
  1931:     cursor = conn.cursor()
  1932:     
  1933:     # Check if table exists
  1934:     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trademc_trades'")
```

#### Line 1934
```python
  1932:     
  1933:     # Check if table exists
  1934:     cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trademc_trades'")
  1935:     if not cursor.fetchone():
  1936:         conn.close()
  1937:         return {"count": 0, "synced": False}
  1938:     
```

#### Line 1939
```python
  1937:         return {"count": 0, "synced": False}
  1938:     
  1939:     cursor.execute('SELECT COUNT(*) FROM trademc_trades')
  1940:     count = cursor.fetchone()[0]
  1941:     
  1942:     if count == 0:
  1943:         conn.close()
```

#### Line 1946
```python
  1944:         return {"count": 0, "synced": False}
  1945:     
  1946:     cursor.execute('SELECT MIN(trade_timestamp), MAX(trade_timestamp) FROM trademc_trades')
  1947:     min_date, max_date = cursor.fetchone()
  1948:     
  1949:     cursor.execute('SELECT SUM(weight) FROM trademc_trades')
  1950:     total_weight = cursor.fetchone()[0] or 0
```

#### Line 1949
```python
  1947:     min_date, max_date = cursor.fetchone()
  1948:     
  1949:     cursor.execute('SELECT SUM(weight) FROM trademc_trades')
  1950:     total_weight = cursor.fetchone()[0] or 0
  1951:     
  1952:     cursor.execute('SELECT MAX(last_synced) FROM trademc_trades')
  1953:     last_synced = cursor.fetchone()[0]
```

#### Line 1952
```python
  1950:     total_weight = cursor.fetchone()[0] or 0
  1951:     
  1952:     cursor.execute('SELECT MAX(last_synced) FROM trademc_trades')
  1953:     last_synced = cursor.fetchone()[0]
  1954:     
  1955:     cursor.execute('SELECT status, COUNT(*) FROM trademc_trades GROUP BY status')
  1956:     status_counts = dict(cursor.fetchall())
```

#### Line 1955
```python
  1953:     last_synced = cursor.fetchone()[0]
  1954:     
  1955:     cursor.execute('SELECT status, COUNT(*) FROM trademc_trades GROUP BY status')
  1956:     status_counts = dict(cursor.fetchall())
  1957:     
  1958:     conn.close()
  1959:     
```

#### Line 1973
```python
  1971: def get_unique_companies() -> List[int]:
  1972:     """Get list of unique company IDs in the database."""
  1973:     conn = sqlite3.connect(DB_PATH)
  1974:     cursor = conn.cursor()
  1975:     
  1976:     cursor.execute('SELECT DISTINCT company_id FROM trademc_trades WHERE company_id IS NOT NULL ORDER BY company_id')
  1977:     companies = [row[0] for row in cursor.fetchall()]
```

#### Line 1976
```python
  1974:     cursor = conn.cursor()
  1975:     
  1976:     cursor.execute('SELECT DISTINCT company_id FROM trademc_trades WHERE company_id IS NOT NULL ORDER BY company_id')
  1977:     companies = [row[0] for row in cursor.fetchall()]
  1978:     
  1979:     conn.close()
  1980:     return companies
```

#### Line 1985
```python
  1983: def get_unique_statuses() -> List[str]:
  1984:     """Get list of unique statuses in the database."""
  1985:     conn = sqlite3.connect(DB_PATH)
  1986:     cursor = conn.cursor()
  1987:     
  1988:     cursor.execute('SELECT DISTINCT status FROM trademc_trades WHERE status IS NOT NULL ORDER BY status')
  1989:     statuses = [row[0] for row in cursor.fetchall()]
```

#### Line 1988
```python
  1986:     cursor = conn.cursor()
  1987:     
  1988:     cursor.execute('SELECT DISTINCT status FROM trademc_trades WHERE status IS NOT NULL ORDER BY status')
  1989:     statuses = [row[0] for row in cursor.fetchall()]
  1990:     
  1991:     conn.close()
  1992:     return statuses
```

#### Line 1998
```python
  1996:     """Get list of unique weight transaction types in the database."""
  1997:     initialize_trademc_table()
  1998:     conn = sqlite3.connect(DB_PATH)
  1999:     cursor = conn.cursor()
  2000: 
  2001:     cursor.execute('SELECT DISTINCT type FROM trademc_weight_transactions WHERE type IS NOT NULL ORDER BY type')
  2002:     types = [row[0] for row in cursor.fetchall()]
```

#### Line 2001
```python
  1999:     cursor = conn.cursor()
  2000: 
  2001:     cursor.execute('SELECT DISTINCT type FROM trademc_weight_transactions WHERE type IS NOT NULL ORDER BY type')
  2002:     types = [row[0] for row in cursor.fetchall()]
  2003: 
  2004:     conn.close()
  2005:     return types
```

#### Line 2019
```python
  2017:     """
  2018:     initialize_trademc_table()
  2019:     conn = sqlite3.connect(DB_PATH)
  2020:     
  2021:     # Search for trades where notes contains the trade number, join with companies
  2022:     query = """
  2023:         SELECT t.*, c.company_name 
```

#### Line 2030
```python
  2028:     """
  2029:     
  2030:     df = pd.read_sql_query(query, conn, params=[f"%{trade_number}%"])
  2031:     conn.close()
  2032:     
  2033:     # Convert timestamp columns
  2034:     for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
```

#### Line 2080
```python
  2078:         return {"success": False, "error": "No companies fetched from API", "count": 0}
  2079:     
  2080:     conn = sqlite3.connect(DB_PATH)
  2081:     cursor = conn.cursor()
  2082:     
  2083:     synced_at = datetime.now().isoformat()
  2084:     inserted = 0
```

#### Line 2089
```python
  2087:     for company in companies:
  2088:         # Check if company already exists
  2089:         cursor.execute('SELECT id FROM trademc_companies WHERE id = ?', (company['id'],))
  2090:         exists = cursor.fetchone()
  2091:         
  2092:         if exists:
  2093:             # Update existing record
```

#### Line 2094
```python
  2092:         if exists:
  2093:             # Update existing record
  2094:             cursor.execute('''
  2095:                 UPDATE trademc_companies SET
  2096:                     status = ?,
  2097:                     company_name = ?,
  2098:                     registration_number = ?,
```

#### Line 2131
```python
  2129:         else:
  2130:             # Insert new record
  2131:             cursor.execute('''
  2132:                 INSERT INTO trademc_companies (
  2133:                     id, status, company_name, registration_number, contact_number,
  2134:                     email_address, sharepoint_identifier, trade_limit, blocked,
  2135:                     vat_number, EVO_customer_code, refining_rate,
```

#### Line 2157
```python
  2155:             inserted += 1
  2156:     
  2157:     commit_with_mirror(conn, DB_PATH)
  2158:     conn.close()
  2159:     
  2160:     return {
  2161:         "success": True,
```

#### Line 2183
```python
  2181: 
  2182:     initialize_trademc_table()
  2183:     conn = sqlite3.connect(DB_PATH)
  2184:     cursor = conn.cursor()
  2185:     
  2186:     cursor.execute('SELECT company_name FROM trademc_companies WHERE id = ?', (company_id,))
  2187:     row = cursor.fetchone()
```

#### Line 2186
```python
  2184:     cursor = conn.cursor()
  2185:     
  2186:     cursor.execute('SELECT company_name FROM trademc_companies WHERE id = ?', (company_id,))
  2187:     row = cursor.fetchone()
  2188:     
  2189:     conn.close()
  2190:     return row[0] if row else None
```

#### Line 2218
```python
  2216:     initialize_trademc_table()
  2217:     
  2218:     conn = sqlite3.connect(DB_PATH)
  2219:     
  2220:     query = """
  2221:         SELECT t.*, c.company_name, c.refining_rate as company_refining_rate
  2222:         FROM trademc_trades t
```

#### Line 2254
```python
  2252:     query += " ORDER BY t.trade_timestamp DESC"
  2253:     
  2254:     df = pd.read_sql_query(query, conn, params=params)
  2255:     conn.close()
  2256:     
  2257:     # Convert timestamp columns
  2258:     for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
```

#### Line 2285
```python
  2283:     initialize_trademc_table()
  2284: 
  2285:     conn = sqlite3.connect(DB_PATH)
  2286: 
  2287:     query = """
  2288:         SELECT w.*, c.company_name
  2289:         FROM trademc_weight_transactions w
```

#### Line 2313
```python
  2311:     query += " ORDER BY w.transaction_timestamp DESC, w.id DESC"
  2312: 
  2313:     df = pd.read_sql_query(query, conn, params=params)
  2314:     conn.close()
  2315: 
  2316:     for col in ['transaction_timestamp', 'date_created', 'date_updated', 'last_synced']:
  2317:         if col in df.columns:
```

#### Line 2331
```python
  2329:     """
  2330:     initialize_trademc_table()
  2331:     conn = sqlite3.connect(DB_PATH)
  2332:     
  2333:     df = pd.read_sql_query(
  2334:         "SELECT * FROM trademc_companies ORDER BY company_name",
  2335:         conn
```

#### Line 2333
```python
  2331:     conn = sqlite3.connect(DB_PATH)
  2332:     
  2333:     df = pd.read_sql_query(
  2334:         "SELECT * FROM trademc_companies ORDER BY company_name",
  2335:         conn
  2336:     )
  2337:     conn.close()
```

## Outbound HTTP/API Calls
### j2-platform/server\scripts\email_pmx_balances.py

#### Line 97
```python
    95:         headers["content-type"] = content_type
    96: 
    97:     resp = requests.get(url, headers=headers, params=params, timeout=max(10, int(timeout)))
    98:     content_type_resp = str(resp.headers.get("Content-Type", "") or "")
    99:     content_disposition = str(resp.headers.get("Content-Disposition", "") or "")
   100:     body_bytes = resp.content if isinstance(resp.content, (bytes, bytearray)) else b""
   101:     is_pdf_content_type = "application/pdf" in content_type_resp.lower()
```

### j2-platform/server\server.py

#### Line 793
```python
   791: 
   792:     try:
   793:         resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
   794:         content_type_resp = resp.headers.get("Content-Type", "")
   795:         parsed: Any = {}
   796:         if "json" in content_type_resp.lower() or str(resp.text or "").lstrip().startswith(("{", "[")):
   797:             try:
```

#### Line 4056
```python
  4054: 
  4055:     try:
  4056:         resp = requests.get(url, headers=headers, params=params, timeout=DAILY_BALANCE_EMAIL_REQUEST_TIMEOUT_SECONDS)
  4057:         body_bytes = resp.content if isinstance(resp.content, (bytes, bytearray)) else b""
  4058:         content_type_resp = str(resp.headers.get("Content-Type", "") or "")
  4059:         content_disposition = str(resp.headers.get("Content-Disposition", "") or "")
  4060:         is_pdf_content_type = "application/pdf" in content_type_resp.lower()
```

### services\rest_service.py

#### Line 35
```python
    33: 
    34:     try:
    35:         resp = requests.post(url, headers=headers, json=payload, timeout=15)
    36:         data = resp.json() if resp.content else {}
    37:         if resp.ok and "accessToken" in data:
    38:             return {
    39:                 "ok": True,
```

#### Line 106
```python
   104: 
   105:     try:
   106:         resp = requests.get(url, headers=headers, params=params, timeout=30)
   107:         content_type = resp.headers.get("Content-Type", "")
   108:         body_text = resp.text
   109: 
   110:         return {
```

#### Line 196
```python
   194: 
   195:     try:
   196:         resp = requests.get(url, headers=headers, params=params, timeout=60)
   197:         content_type = resp.headers.get("Content-Type", "")
   198:         body_text = resp.text
   199: 
   200:         return {
```

#### Line 268
```python
   266: 
   267:     try:
   268:         resp = requests.get(url, headers=headers, params=params, timeout=60)
   269:         content_type = resp.headers.get("Content-Type", "")
   270:         body_text = resp.text
   271: 
   272:         return {
```

#### Line 453
```python
   451: 
   452:     try:
   453:         resp = requests.get(url, headers=headers, params=params, timeout=60)
   454:         content_type = resp.headers.get("Content-Type", "")
   455:         body_text = resp.text
   456: 
   457:         return {
```

#### Line 479
```python
   477: 
   478:     try:
   479:         resp = requests.get(download_url, headers=headers, timeout=120)
   480:         content_type = resp.headers.get("Content-Type", "")
   481:         return {
   482:             "ok": resp.ok,
   483:             "status": resp.status_code,
```

#### Line 554
```python
   552: 
   553:     try:
   554:         resp = requests.get(url, headers=headers, params=params, timeout=timeout)
   555:         content_type_resp = resp.headers.get("Content-Type", "")
   556:         body_text = resp.text if isinstance(resp.text, str) else ""
   557: 
   558:         parsed_json: Any = None
```

#### Line 674
```python
   672: 
   673:     try:
   674:         resp = requests.get(url, headers=headers, params=params, timeout=timeout)
   675:         content_type = resp.headers.get("Content-Type", "")
   676:         body_text = resp.text if isinstance(resp.text, str) else ""
   677: 
   678:         parsed_json: Any = None
```

#### Line 788
```python
   786: 
   787:     try:
   788:         resp = requests.get(url, headers=headers, params=params, timeout=timeout)
   789:         content_type_resp = resp.headers.get("Content-Type", "")
   790:         body_text = resp.text if isinstance(resp.text, str) else ""
   791: 
   792:         parsed_json: Any = None
```

#### Line 1023
```python
  1021: 
  1022:     try:
  1023:         resp = requests.get(url, headers=headers, params=params, timeout=timeout)
  1024:         content_type_resp = resp.headers.get("Content-Type", "")
  1025:         content_disposition = resp.headers.get("Content-Disposition", "")
  1026:         body_bytes = resp.content if isinstance(resp.content, (bytes, bytearray)) else b""
  1027:         body_text = ""
```

### services\trademc_service.py

#### Line 280
```python
   278: 
   279:     try:
   280:         response = requests.get(url, headers=get_api_headers(), params=params, timeout=30)
   281:         if response.status_code == 200:
   282:             return response.json()
   283:         print(f"API Error: {response.status_code} - {response.text}")
   284:         return None
```

#### Line 536
```python
   534:         params["_cb"] = int(time.time() * 1000) + attempt
   535:         try:
   536:             response = requests.get(url, headers=get_api_headers(), params=params, timeout=45)
   537:         except requests.exceptions.RequestException as e:
   538:             last_error = f"Request failed: {e}"
   539:             print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
   540:             if attempt < max_attempts - 1:
```

#### Line 625
```python
   623:         for attempt in range(max_attempts):
   624:             try:
   625:                 res = requests.patch(url, headers=headers, json=payload, timeout=30)
   626:                 return res, ""
   627:             except requests.exceptions.RequestException as exc:
   628:                 last_err = f"request failed: {exc}"
   629:             except OSError as exc:
```

#### Line 907
```python
   905:     }
   906:     try:
   907:         res = requests.get(url, headers=get_api_headers(), params=params, timeout=30)
   908:         if res.status_code != 200:
   909:             return {"ok": False, "error": f"Directus HTTP {res.status_code}", "details": str(res.text or "")[:500]}
   910:         payload = res.json() if res.text else {}
   911:     except requests.exceptions.RequestException as exc:
```

#### Line 1609
```python
  1607: 
  1608:     try:
  1609:         response = requests.get(url, headers=get_api_headers(), params=params, timeout=30)
  1610: 
  1611:         if response.status_code == 200:
  1612:             return response.json()
  1613:         else:
```

#### Line 1656
```python
  1654:     url = f"{_trademc_base_url()}/items/weight_transaction_ledger"
  1655:     try:
  1656:         response = requests.get(
  1657:             url,
  1658:             headers=get_api_headers(),
  1659:             params={
  1660:                 "limit": 1,
```

#### Line 2051
```python
  2049:     
  2050:     try:
  2051:         response = requests.get(url, headers=get_api_headers(), timeout=30)
  2052:         
  2053:         if response.status_code == 200:
  2054:             result = response.json()
  2055:             return result.get("data", [])
```

## Flask API Routes
### j2-platform/server\server.py

#### Line 6815
```python
  6813: 
  6814: 
  6815: @app.route("/api/health")
  6816: def health():
  6817:     return jsonify({"status": "ok", "time": datetime.now().isoformat(), "build": API_BUILD})
  6818: 
  6819: 
```

#### Line 6820
```python
  6818: 
  6819: 
  6820: @app.route("/api/auth/login", methods=["POST"])
  6821: def auth_login():
  6822:     data = request.json or {}
  6823:     username = _pmx_non_empty(data.get("username"), data.get("email"))
  6824:     password = str(data.get("password", "") or "")
```

#### Line 6840
```python
  6838: 
  6839: 
  6840: @app.route("/api/auth/me")
  6841: def auth_me():
  6842:     user = _auth_request_user()
  6843:     if not user:
  6844:         return jsonify({"ok": False, "error": "Not authenticated"}), 401
```

#### Line 6848
```python
  6846: 
  6847: 
  6848: @app.route("/api/auth/logout", methods=["POST"])
  6849: def auth_logout():
  6850:     res = jsonify({"ok": True})
  6851:     _auth_clear_cookie(res)
  6852:     return res
```

#### Line 6855
```python
  6853: 
  6854: 
  6855: @app.route("/api/auth/users", methods=["GET"])
  6856: def auth_users():
  6857:     current_user = _auth_request_user()
  6858:     if not current_user or not bool(current_user.get("is_admin")):
  6859:         return jsonify({"ok": False, "error": "Admin access required"}), 403
```

#### Line 6864
```python
  6862: 
  6863: 
  6864: @app.route("/api/auth/users", methods=["POST"])
  6865: def auth_create_user():
  6866:     current_user = _auth_request_user()
  6867:     if not current_user or not bool(current_user.get("is_admin")):
  6868:         return jsonify({"ok": False, "error": "Admin access required"}), 403
```

#### Line 6933
```python
  6931: 
  6932: 
  6933: @app.route("/api/auth/users/<int:user_id>", methods=["PUT"])
  6934: def auth_update_user(user_id):
  6935:     current_user = _auth_request_user()
  6936:     if not current_user or not bool(current_user.get("is_admin")):
  6937:         return jsonify({"ok": False, "error": "Admin access required"}), 403
```

#### Line 7054
```python
  7052: 
  7053: 
  7054: @app.route("/api/auth/users/<int:user_id>", methods=["DELETE"])
  7055: def auth_delete_user(user_id):
  7056:     current_user = _auth_request_user()
  7057:     if not current_user or not bool(current_user.get("is_admin")):
  7058:         return jsonify({"ok": False, "error": "Admin access required"}), 403
```

#### Line 7097
```python
  7095: 
  7096: 
  7097: @app.route("/api/trades")
  7098: def get_trades():
  7099:     df = load_all_trades()
  7100:     # Convert dates to strings for JSON
  7101:     for col in df.columns:
```

#### Line 7107
```python
  7105: 
  7106: 
  7107: @app.route("/api/trades", methods=["POST"])
  7108: def add_trade():
  7109:     data = request.json
  7110:     result = add_new_trade(data)
  7111:     return jsonify({"success": result})
```

#### Line 7114
```python
  7112: 
  7113: 
  7114: @app.route("/api/trades/<int:trade_id>", methods=["PATCH"])
  7115: def patch_trade(trade_id):
  7116:     data = request.json
  7117:     order_id = data.get("order_id", "")
  7118:     result = update_trade_order_id(trade_id, order_id)
```

#### Line 7124
```python
  7122: 
  7123: 
  7124: @app.route("/api/trades/backup", methods=["POST"])
  7125: def backup_trades_endpoint():
  7126:     """Manual trigger to backup trade assignments to JSON."""
  7127:     success = backup_manual_trades_to_json()
  7128:     if success:
```

#### Line 7134
```python
  7132: 
  7133: 
  7134: @app.route("/api/trades/<int:trade_id>/trade-number", methods=["PUT"])
  7135: def update_trade_number(trade_id):
  7136:     data = request.json or {}
  7137:     new_trade_num = normalize_trade_number(data.get("trade_number", ""))
  7138:     try:
```

#### Line 7155
```python
  7153: 
  7154: 
  7155: @app.route("/api/trades/ledger")
  7156: def get_ledger():
  7157:     df = load_all_trades()
  7158:     ledger = build_ledger_view(df)
  7159:     ledger = _apply_ledger_filters(ledger, request.args)
```

#### Line 7194
```python
  7192: 
  7193: 
  7194: @app.route("/api/pmx/sync-ledger", methods=["POST"])
  7195: def sync_pmx_ledger():
  7196:     data = request.json or {}
  7197:     result = sync_pmx_trades_to_db(data, request.headers)
  7198:     if bool(result.get("ok")):
```

#### Line 7205
```python
  7203: 
  7204: 
  7205: @app.route("/api/pmx/trades/<int:trade_id>/trade-number", methods=["PUT"])
  7206: def update_pmx_trade_number(trade_id):
  7207:     data = request.json or {}
  7208:     new_trade_num = normalize_trade_number(data.get("trade_number", ""))
  7209:     try:
```

#### Line 7226
```python
  7224: 
  7225: 
  7226: @app.route("/api/pmx/ledger")
  7227: def get_pmx_ledger():
  7228:     sync_flag = str(request.args.get("sync", "")).strip().lower()
  7229:     if sync_flag in {"1", "true", "yes", "y"}:
  7230:         current_user = getattr(g, "current_user", None)
```

#### Line 7244
```python
  7242: 
  7243: 
  7244: @app.route("/api/pmx/ledger-full-csv")
  7245: def get_pmx_ledger_full_csv():
  7246:     recon_result = _get_pmx_reconciliation_inner()
  7247:     recon_response = recon_result[0] if isinstance(recon_result, tuple) else recon_result
  7248:     recon_status = recon_result[1] if isinstance(recon_result, tuple) and len(recon_result) > 1 else getattr(recon_response, "status_code", 200)
```

#### Line 7901
```python
  7899: 
  7900: 
  7901: @app.route("/api/pmx/reconciliation")
  7902: def get_pmx_reconciliation():
  7903:     """Return PMX statement reconciliation data as JSON for the XAU Reconciliation tab."""
  7904:     try:
  7905:         return _get_pmx_reconciliation_inner()
```

#### Line 9462
```python
  9460: 
  9461: 
  9462: @app.route("/api/pmx/fnc-pdf", methods=["GET", "POST"])
  9463: def get_pmx_fnc_pdf():
  9464:     data = request.args.to_dict() if request.method == "GET" else (request.json or {})
  9465:     cell = str(data.get("cell") or data.get("fnc") or data.get("fnc_number") or "").strip()
  9466:     if not cell:
```

#### Line 9492
```python
  9490: 
  9491: 
  9492: @app.route("/api/trades/open-positions")
  9493: def get_open_positions():
  9494:     df = load_all_trades()
  9495:     records, summary = build_open_positions(df)
  9496:     return jsonify({"positions": records, "summary": summary})
```

#### Line 9499
```python
  9497: 
  9498: 
  9499: @app.route("/api/pmx/open-positions-reval")
  9500: def get_pmx_open_positions_reval():
  9501:     args_dict = request.args.to_dict()
  9502:     cache_key = _build_cache_key("pmx_open_positions_reval", args_dict)
  9503:     result = _get_cached_heavy_result(
```

#### Line 9510
```python
  9508: 
  9509: 
  9510: @app.route("/api/pmx/open-positions-reval/pdf")
  9511: def get_pmx_open_positions_reval_pdf():
  9512:     args_dict = request.args.to_dict()
  9513:     try:
  9514:         pdf_bytes = build_open_positions_reval_pdf(args_dict, request.headers)
```

#### Line 9527
```python
  9525: 
  9526: 
  9527: @app.route("/api/pmx/account-balances")
  9528: def get_pmx_account_balances():
  9529:     result = _fetch_open_positions_account_balances(request.args.to_dict(), request.headers)
  9530:     return jsonify(_json_safe(result))
  9531: 
```

#### Line 9533
```python
  9531: 
  9532: 
  9533: @app.route("/api/pmx/account-recon")
  9534: def get_pmx_account_recon():
  9535:     args_dict = request.args.to_dict()
  9536:     cache_key = _build_cache_key("account_recon", args_dict)
  9537:     result = _get_cached_heavy_result(
```

#### Line 9544
```python
  9542: 
  9543: 
  9544: @app.route("/api/pmx/account-recon/opening-balance", methods=["POST"])
  9545: def set_pmx_account_recon_opening_balance():
  9546:     body = request.get_json(silent=True) or {}
  9547:     month = str(body.get("month", "") or "").strip()
  9548:     currency = str(body.get("currency", "") or "").strip().upper()
```

#### Line 9575
```python
  9573: 
  9574: 
  9575: @app.route("/api/pmx/account-recon/opening-balances")
  9576: def get_pmx_account_recon_opening_balances():
  9577:     try:
  9578:         conn = get_pmx_db_connection()
  9579:         cur = conn.cursor()
```

#### Line 9591
```python
  9589: 
  9590: 
  9591: @app.route("/api/pmx/forward-exposure")
  9592: def get_pmx_forward_exposure():
  9593:     args_dict = request.args.to_dict()
  9594:     cache_key = _build_cache_key("pmx_forward_exposure", args_dict)
  9595:     result = _get_cached_heavy_result(
```

#### Line 9602
```python
  9600: 
  9601: 
  9602: @app.route("/api/trademc/trades")
  9603: def get_trademc_trades():
  9604:     kwargs = {}
  9605:     for key in ["status", "ref_filter", "company_id", "start_date", "end_date"]:
  9606:         val = request.args.get(key)
```

#### Line 9660
```python
  9658: 
  9659: 
  9660: @app.route("/api/trademc/sync", methods=["POST"])
  9661: def sync_trademc():
  9662:     global _trademc_sync_status
  9663:     data = request.json or {}
  9664:     include_weight = _pmx_bool(data.get("include_weight"), default=False)
```

#### Line 9729
```python
  9727: 
  9728: 
  9729: @app.route("/api/trademc/sync/status", methods=["GET"])
  9730: def sync_trademc_status():
  9731:     with _trademc_sync_lock:
  9732:         return jsonify(_json_safe(_trademc_sync_status))
  9733: 
```

#### Line 9735
```python
  9733: 
  9734: 
  9735: @app.route("/api/admin/clean-pipeline", methods=["GET", "POST"])
  9736: def clean_pipeline_status():
  9737:     if request.method == "GET":
  9738:         with CLEAN_PIPELINE_LOCK:
  9739:             return jsonify(_json_safe({"ok": True, "state": dict(_CLEAN_PIPELINE_STATE)}))
```

#### Line 9753
```python
  9751: 
  9752: 
  9753: @app.route("/api/trademc/diagnostics", methods=["GET"])
  9754: def trademc_diagnostics():
  9755:     trade_id_raw = str(request.args.get("trade_id", "") or "").strip()
  9756:     trade_id = int(trade_id_raw) if trade_id_raw.isdigit() else None
  9757: 
```

#### Line 9777
```python
  9775: 
  9776: 
  9777: @app.route("/api/trademc/trades/<int:trade_id>/ref-number", methods=["PUT"])
  9778: def update_trademc_ref_number(trade_id):
  9779:     data = request.json or {}
  9780:     ref_number = str(data.get("ref_number", data.get("trade_number", "")) or "").strip()
  9781:     try:
```

#### Line 9795
```python
  9793: 
  9794: 
  9795: @app.route("/api/trademc/companies")
  9796: def get_companies():
  9797:     df = get_all_companies_df()
  9798:     return jsonify(df.fillna("").to_dict(orient="records"))
  9799: 
```

#### Line 9801
```python
  9799: 
  9800: 
  9801: @app.route("/api/trademc/stats")
  9802: def trademc_stats():
  9803:     stats = get_trademc_stats()
  9804:     return jsonify(stats)
  9805: 
```

#### Line 9807
```python
  9805: 
  9806: 
  9807: @app.route("/api/trademc/live-prices")
  9808: def get_trademc_live_prices():
  9809:     force_refresh = _is_truthy(request.args.get("force"))
  9810:     result = _get_cached_trademc_live_prices(force_refresh=force_refresh)
  9811:     if not bool(result.get("ok")):
```

#### Line 9816
```python
  9814: 
  9815: 
  9816: @app.route("/api/trademc/weight-transactions")
  9817: def get_weight_transactions():
  9818:     kwargs = {}
  9819:     for key in ["company_id", "type", "start_date", "end_date"]:
  9820:         val = request.args.get(key)
```

#### Line 9833
```python
  9831: 
  9832: 
  9833: @app.route("/api/trademc/weight-types")
  9834: def get_weight_types():
  9835:     return jsonify(get_unique_weight_types())
  9836: 
  9837: 
```

#### Line 9838
```python
  9836: 
  9837: 
  9838: @app.route("/api/trademc/sync-weight", methods=["POST"])
  9839: def sync_weight():
  9840:     result = sync_trademc_weight_transactions()
  9841:     fiscal_purge = _purge_pre_fiscal_rows(cutoff_iso=FISCAL_TRADES_START_DATE, purge_fx=True, purge_pmx=False)
  9842:     result["fiscal_cutoff"] = FISCAL_TRADES_START_DATE
```

#### Line 9852
```python
  9850: 
  9851: 
  9852: @app.route("/api/hedging")
  9853: def get_hedging():
  9854:     rows = _get_cached_heavy_result(
  9855:         "hedging:pmx",
  9856:         lambda: build_hedging_comparison(),
```

#### Line 9861
```python
  9859: 
  9860: 
  9861: @app.route("/api/weighted-average/<trade_num>")
  9862: def get_weighted_average(trade_num):
  9863:     result = build_weighted_average(trade_num)
  9864:     if result is None:
  9865:         return jsonify({"error": "No data found"}), 404
```

#### Line 9869
```python
  9867: 
  9868: 
  9869: @app.route("/api/ticket/<trade_num>")
  9870: def get_ticket(trade_num):
  9871:     result = build_trading_ticket(trade_num)
  9872:     if result is None:
  9873:         return jsonify({"error": "No data found"}), 404
```

#### Line 9877
```python
  9875: 
  9876: 
  9877: @app.route("/api/ticket/<trade_num>/pdf")
  9878: def get_ticket_pdf(trade_num):
  9879:     frames = build_trading_ticket_frames(trade_num)
  9880:     if frames is None:
  9881:         return jsonify({"error": "No data found"}), 404
```

#### Line 9904
```python
  9902: 
  9903: 
  9904: @app.route("/api/profit/monthly")
  9905: def get_profit_monthly():
  9906:     result = _get_cached_heavy_result(
  9907:         "profit_monthly",
  9908:         build_profit_monthly_report,
```

#### Line 9913
```python
  9911: 
  9912: 
  9913: @app.route("/api/export-trades/save", methods=["POST"])
  9914: def export_trades_save_to_folder():
  9915:     data = request.json or {}
  9916:     trades_payload = data.get("trades", [])
  9917:     if not isinstance(trades_payload, list) or len(trades_payload) == 0:
```

#### Line 10080
```python
 10078: 
 10079: 
 10080: @app.route("/api/export/ledger")
 10081: def export_ledger():
 10082:     fmt = request.args.get("format", "csv")
 10083:     df = load_all_trades()
 10084:     ledger = build_ledger_view(df)
```


