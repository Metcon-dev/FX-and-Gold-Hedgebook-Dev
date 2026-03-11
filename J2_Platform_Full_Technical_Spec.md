# J2 Platform Full Technical Specification
_Generated: 2026-03-11 14:34:45_

## 1. System Overview
| Area | Implementation |
|---|---|
| Frontend | `j2-platform/client` (React + TypeScript + Vite) |
| Backend API | `j2-platform/server/server.py` (Flask + CORS) |
| Domain/Data services | `services/*.py`, `models/*.py` |
| Primary storage | SQLite (`fx_trading_ledger.db`, `pmx_database.db`) |
| Integrity warehouse | `j2_platform_clean.db` via clean pipeline |

## 2. Data Sources and Ingestion
| Source | Pull Mechanism | Landing Storage |
|---|---|---|
| PMX Deal Report | `/user/alldealFilter_report` (PMX API calls in `services/rest_service.py`) | `pmx_database.db.trades` |
| PMX Account Statement | `/user/account_statementReport` | Used for reconciliation/report endpoints |
| PMX Account Balances | `/user/loadAccount` | API responses used by account balances/recon views |
| PMX Fixing Invoice PDFs | `/user/export_FixInvoice_pdf` | Download/export outputs |
| TradeMC Directus | HTTP API via `services/trademc_service.py` | `fx_trading_ledger.db` TradeMC tables |
| Manual user inputs | UI/API mutations (trade tags, opening balances, etc.) | SQLite + `manual_trades.json` backup |

## 3. Frontend Specification (Tabs/Modules)
| Tab ID | Label |
|---|---|
| `pmx_ledger` | PMX Ledger |
| `hedging` | Hedging |
| `forward_exposure` | Forward Exposure |
| `open_positions_reval` | Open Positions Reval |
| `profit` | Profit |
| `trademc` | TradeMC Trades |
| `suppliers` | Supplier Balances |
| `export_trades` | Export Trades |
| `ticket` | Trading Ticket |
| `user_management` | User Management |

## 4. Backend API Specification
Total discovered Flask routes: **46**

| Methods | Path | Handler | Source Line |
|---|---|---|---|
| `GET` | `/api/health` | `health` | 5988 |
| `POST` | `/api/auth/login` | `auth_login` | 5993 |
| `GET` | `/api/auth/me` | `auth_me` | 6013 |
| `POST` | `/api/auth/logout` | `auth_logout` | 6021 |
| `GET` | `/api/auth/users` | `auth_users` | 6028 |
| `POST` | `/api/auth/users` | `auth_create_user` | 6037 |
| `PUT` | `/api/auth/users/<int:user_id>` | `auth_update_user` | 6106 |
| `DELETE` | `/api/auth/users/<int:user_id>` | `auth_delete_user` | 6227 |
| `GET` | `/api/trades` | `get_trades` | 6271 |
| `POST` | `/api/trades` | `add_trade` | 6281 |
| `PATCH` | `/api/trades/<int:trade_id>` | `patch_trade` | 6288 |
| `POST` | `/api/trades/backup` | `backup_trades_endpoint` | 6299 |
| `PUT` | `/api/trades/<int:trade_id>/trade-number` | `update_trade_number` | 6309 |
| `GET` | `/api/trades/ledger` | `get_ledger` | 6330 |
| `POST` | `/api/pmx/sync-ledger` | `sync_pmx_ledger` | 6369 |
| `PUT` | `/api/pmx/trades/<int:trade_id>/trade-number` | `update_pmx_trade_number` | 6380 |
| `GET` | `/api/pmx/ledger` | `get_pmx_ledger` | 6401 |
| `GET` | `/api/pmx/ledger-full-csv` | `get_pmx_ledger_full_csv` | 6419 |
| `GET` | `/api/pmx/reconciliation` | `get_pmx_reconciliation` | 7076 |
| `GET,POST` | `/api/pmx/fnc-pdf` | `get_pmx_fnc_pdf` | 8654 |
| `GET` | `/api/trades/open-positions` | `get_open_positions` | 8684 |
| `GET` | `/api/pmx/open-positions-reval` | `get_pmx_open_positions_reval` | 8692 |
| `GET` | `/api/pmx/account-balances` | `get_pmx_account_balances` | 8703 |
| `GET` | `/api/pmx/account-recon` | `get_pmx_account_recon` | 8709 |
| `POST` | `/api/pmx/account-recon/opening-balance` | `set_pmx_account_recon_opening_balance` | 8720 |
| `GET` | `/api/pmx/account-recon/opening-balances` | `get_pmx_account_recon_opening_balances` | 8751 |
| `GET` | `/api/pmx/forward-exposure` | `get_pmx_forward_exposure` | 8767 |
| `GET` | `/api/trademc/trades` | `get_trademc_trades` | 8778 |
| `POST` | `/api/trademc/sync` | `sync_trademc` | 8837 |
| `GET` | `/api/trademc/sync/status` | `sync_trademc_status` | 8906 |
| `GET,POST` | `/api/admin/clean-pipeline` | `clean_pipeline_status` | 8912 |
| `GET` | `/api/trademc/diagnostics` | `trademc_diagnostics` | 8930 |
| `PUT` | `/api/trademc/trades/<int:trade_id>/ref-number` | `update_trademc_ref_number` | 8954 |
| `GET` | `/api/trademc/companies` | `get_companies` | 8972 |
| `GET` | `/api/trademc/stats` | `trademc_stats` | 8978 |
| `GET` | `/api/trademc/live-prices` | `get_trademc_live_prices` | 8984 |
| `GET` | `/api/trademc/weight-transactions` | `get_weight_transactions` | 8993 |
| `GET` | `/api/trademc/weight-types` | `get_weight_types` | 9010 |
| `POST` | `/api/trademc/sync-weight` | `sync_weight` | 9015 |
| `GET` | `/api/hedging` | `get_hedging` | 9030 |
| `GET` | `/api/weighted-average/<trade_num>` | `get_weighted_average` | 9040 |
| `GET` | `/api/ticket/<trade_num>` | `get_ticket` | 9049 |
| `GET` | `/api/ticket/<trade_num>/pdf` | `get_ticket_pdf` | 9057 |
| `GET` | `/api/profit/monthly` | `get_profit_monthly` | 9082 |
| `POST` | `/api/export-trades/save` | `export_trades_save_to_folder` | 9091 |
| `GET` | `/api/export/ledger` | `export_ledger` | 9256 |

## 5. Configuration Specification (Environment Variables)
Total discovered env keys: **58**

| Env Key | Purpose (in code) |
|---|---|
| `APP_AUTH_COOKIE_SECURE` | Runtime/server configuration |
| `APP_AUTH_DISPLAY_NAME` | Runtime/server configuration |
| `APP_AUTH_FORCE_RESEED` | Runtime/server configuration |
| `APP_AUTH_PASSWORD` | Runtime/server configuration |
| `APP_AUTH_PBKDF2_ITERATIONS` | Runtime/server configuration |
| `APP_AUTH_SECRET` | Runtime/server configuration |
| `APP_AUTH_SESSION_SECONDS` | Runtime/server configuration |
| `APP_AUTH_USERNAME` | Runtime/server configuration |
| `APP_READONLY_DISPLAY_NAME` | Runtime/server configuration |
| `APP_READONLY_PASSWORD` | Runtime/server configuration |
| `APP_READONLY_USERNAME` | Runtime/server configuration |
| `APP_VIEWER_PASSWORD` | Runtime/server configuration |
| `APP_VIEWER_USERNAME` | Runtime/server configuration |
| `BALANCE_EMAIL_SUBJECT_PREFIX` | Runtime/server configuration |
| `BALANCE_EMAIL_TO` | Runtime/server configuration |
| `COMPUTERNAME` | Runtime/server configuration |
| `FISCAL_TRADES_START_DATE` | Runtime/server configuration |
| `FLASK_SECRET_KEY` | Runtime/server configuration |
| `HEAVY_ROUTE_CACHE_TTL_SECONDS` | Runtime/server configuration |
| `PMX_ACC_OPT_KEY` | Runtime/server configuration |
| `PMX_API_HOST` | Runtime/server configuration |
| `PMX_BALANCE_PDF_PATH` | Runtime/server configuration |
| `PMX_CACHE_CONTROL` | Runtime/server configuration |
| `PMX_CONTENT_TYPE` | Runtime/server configuration |
| `PMX_CREATED_BY` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_CHECK_INTERVAL_SECONDS` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_ENABLED` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_HOUR` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_MINUTE` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_REQUEST_TIMEOUT_SECONDS` | Runtime/server configuration |
| `PMX_DAILY_BALANCE_EMAIL_RETRY_SECONDS` | Runtime/server configuration |
| `PMX_EXPORT_TRADES_DIR` | Runtime/server configuration |
| `PMX_HISTORY_START_DATE` | Runtime/server configuration |
| `PMX_LOCATION` | Runtime/server configuration |
| `PMX_LOGIN_FORCED` | Runtime/server configuration |
| `PMX_LOGIN_LOCATION` | Runtime/server configuration |
| `PMX_LOGIN_PASSWORD` | Runtime/server configuration |
| `PMX_LOGIN_PATH` | Runtime/server configuration |
| `PMX_LOGIN_TIMEOUT` | Runtime/server configuration |
| `PMX_LOGIN_USERNAME` | Runtime/server configuration |
| `PMX_PLATFORM` | Runtime/server configuration |
| `PMX_SID` | Runtime/server configuration |
| `PMX_TRADE_NAME` | Runtime/server configuration |
| `PMX_USERNAME` | Runtime/server configuration |
| `PMX_X_AUTH` | Runtime/server configuration |
| `SMTP_FROM` | Runtime/server configuration |
| `SMTP_HOST` | Runtime/server configuration |
| `SMTP_PASSWORD` | Runtime/server configuration |
| `SMTP_PORT` | Runtime/server configuration |
| `SMTP_SSL` | Runtime/server configuration |
| `SMTP_STARTTLS` | Runtime/server configuration |
| `SMTP_USER` | Runtime/server configuration |
| `STONEX_ACCESS_TOKEN` | Runtime/server configuration |
| `STONEX_PASSWORD` | Runtime/server configuration |
| `STONEX_SUBSCRIPTION_KEY` | Runtime/server configuration |
| `STONEX_USERNAME` | Runtime/server configuration |
| `TRADEMC_LIVE_PRICES_SAMPLE_SIZE` | Runtime/server configuration |
| `TRADEMC_LIVE_PRICES_TTL_SECONDS` | Runtime/server configuration |

## 6. Database Storage Locations
| DB File | Exists | Size (MB) | Notes |
|---|---:|---:|---|
| `c:\Users\Joshua.Kress\Desktop\JupyterProject2\fx_trading_ledger.db` | Yes | 17.742 | Primary |
| `c:\Users\Joshua.Kress\Desktop\JupyterProject2\pmx_database.db` | Yes | 13.18 | Primary |
| `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2_platform_clean.db` | No | 0 | Primary |
| `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2-platform\server\fx_trading_ledger.db` | Yes | 0.0 | Server-subdir copy/placeholder |
| `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2-platform\server\pmx_database.db` | Yes | 0.0 | Server-subdir copy/placeholder |

## 7. Database Full Specs (Tables, Columns, Indexes, Row Counts)
### 7.1 `c:\Users\Joshua.Kress\Desktop\JupyterProject2\fx_trading_ledger.db`
Size: **17.742 MB**

| Table | Rows |
|---|---:|
| `app_users` | 4 |
| `scheduled_job_runs` | 8 |
| `sqlite_sequence` | 3 |
| `trademc_companies` | 58 |
| `trademc_trades` | 88 |
| `trademc_weight_transactions` | 21 |
| `trades` | 0 |

#### Table `app_users`
Row count: **4**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `username` | `TEXT` | 1 | 0 | `` |
| `display_name` | `TEXT` | 0 | 0 | `` |
| `password_hash` | `TEXT` | 1 | 0 | `` |
| `role` | `TEXT` | 1 | 0 | `'admin'` |
| `can_read` | `INTEGER` | 1 | 0 | `1` |
| `can_write` | `INTEGER` | 1 | 0 | `1` |
| `is_admin` | `INTEGER` | 1 | 0 | `1` |
| `is_active` | `INTEGER` | 1 | 0 | `1` |
| `created_at` | `TIMESTAMP` | 0 | 0 | `CURRENT_TIMESTAMP` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_app_users_username_lower` | Yes | `None` |
| `idx_app_users_username` | Yes | `username` |
| `sqlite_autoindex_app_users_1` | Yes | `username` |

#### Table `scheduled_job_runs`
Row count: **8**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `job_name` | `TEXT` | 1 | 0 | `` |
| `run_date` | `TEXT` | 1 | 0 | `` |
| `status` | `TEXT` | 1 | 0 | `` |
| `message` | `TEXT` | 0 | 0 | `` |
| `created_at` | `TEXT` | 1 | 0 | `datetime('now')` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_scheduled_job_runs_lookup` | No | `job_name`, `run_date`, `status`, `created_at` |

#### Table `sqlite_sequence`
Row count: **3**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `name` | `` | 0 | 0 | `` |
| `seq` | `` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| _(none)_ | - | - |

#### Table `trademc_companies`
Row count: **58**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `status` | `TEXT` | 0 | 0 | `` |
| `company_name` | `TEXT` | 0 | 0 | `` |
| `registration_number` | `TEXT` | 0 | 0 | `` |
| `contact_number` | `TEXT` | 0 | 0 | `` |
| `email_address` | `TEXT` | 0 | 0 | `` |
| `sharepoint_identifier` | `TEXT` | 0 | 0 | `` |
| `trade_limit` | `REAL` | 0 | 0 | `` |
| `blocked` | `INTEGER` | 0 | 0 | `0` |
| `vat_number` | `TEXT` | 0 | 0 | `` |
| `EVO_customer_code` | `TEXT` | 0 | 0 | `` |
| `refining_rate` | `REAL` | 0 | 0 | `` |
| `date_created` | `DATETIME` | 0 | 0 | `` |
| `date_updated` | `DATETIME` | 0 | 0 | `` |
| `last_synced` | `DATETIME` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_trademc_company_name` | No | `company_name` |

#### Table `trademc_trades`
Row count: **88**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `status` | `TEXT` | 0 | 0 | `` |
| `company_id` | `INTEGER` | 0 | 0 | `` |
| `weight` | `REAL` | 0 | 0 | `` |
| `notes` | `TEXT` | 0 | 0 | `` |
| `ref_number` | `TEXT` | 0 | 0 | `` |
| `trade_timestamp` | `DATETIME` | 0 | 0 | `` |
| `zar_per_troy_ounce` | `REAL` | 0 | 0 | `` |
| `zar_to_usd` | `REAL` | 0 | 0 | `` |
| `requested_zar_per_gram` | `REAL` | 0 | 0 | `` |
| `zar_per_troy_ounce_confirmed` | `REAL` | 0 | 0 | `` |
| `zar_to_usd_confirmed` | `REAL` | 0 | 0 | `` |
| `usd_per_troy_ounce_confirmed` | `REAL` | 0 | 0 | `` |
| `date_created` | `DATETIME` | 0 | 0 | `` |
| `date_updated` | `DATETIME` | 0 | 0 | `` |
| `evo_exported` | `INTEGER` | 0 | 0 | `0` |
| `last_synced` | `DATETIME` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_trademc_company` | No | `company_id` |
| `idx_trademc_timestamp` | No | `trade_timestamp` |
| `idx_trademc_notes` | No | `notes` |
| `idx_trademc_status` | No | `status` |

#### Table `trademc_weight_transactions`
Row count: **21**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `user_created` | `TEXT` | 0 | 0 | `` |
| `date_created` | `DATETIME` | 0 | 0 | `` |
| `user_updated` | `TEXT` | 0 | 0 | `` |
| `date_updated` | `DATETIME` | 0 | 0 | `` |
| `company_id` | `INTEGER` | 0 | 0 | `` |
| `pc_code` | `TEXT` | 0 | 0 | `` |
| `notes` | `TEXT` | 0 | 0 | `` |
| `type` | `TEXT` | 0 | 0 | `` |
| `weight` | `REAL` | 0 | 0 | `` |
| `rolling_balance` | `REAL` | 0 | 0 | `` |
| `transaction_timestamp` | `DATETIME` | 0 | 0 | `` |
| `gold_percentage` | `REAL` | 0 | 0 | `` |
| `old_id` | `INTEGER` | 0 | 0 | `` |
| `reversal_id` | `INTEGER` | 0 | 0 | `` |
| `trade_id` | `INTEGER` | 0 | 0 | `` |
| `last_synced` | `DATETIME` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_trademc_weight_trade` | No | `trade_id` |
| `idx_trademc_weight_type` | No | `type` |
| `idx_trademc_weight_timestamp` | No | `transaction_timestamp` |
| `idx_trademc_weight_company` | No | `company_id` |

#### Table `trades`
Row count: **0**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `trade_date` | `DATE` | 1 | 0 | `` |
| `value_date` | `DATE` | 1 | 0 | `` |
| `symbol` | `TEXT` | 1 | 0 | `` |
| `side` | `TEXT` | 1 | 0 | `` |
| `narration` | `TEXT` | 0 | 0 | `` |
| `quantity` | `REAL` | 1 | 0 | `` |
| `price` | `REAL` | 1 | 0 | `` |
| `settle_currency` | `TEXT` | 1 | 0 | `` |
| `settle_amount` | `REAL` | 1 | 0 | `` |
| `doc_number` | `TEXT` | 0 | 0 | `` |
| `clord_id` | `TEXT` | 0 | 0 | `` |
| `order_id` | `TEXT` | 0 | 0 | `` |
| `debit_eur` | `REAL` | 0 | 0 | `0` |
| `credit_eur` | `REAL` | 0 | 0 | `0` |
| `debit_usd` | `REAL` | 0 | 0 | `0` |
| `credit_usd` | `REAL` | 0 | 0 | `0` |
| `debit_zar` | `REAL` | 0 | 0 | `0` |
| `credit_zar` | `REAL` | 0 | 0 | `0` |
| `debit_xau` | `REAL` | 0 | 0 | `0` |
| `credit_xau` | `REAL` | 0 | 0 | `0` |
| `debit_xag` | `REAL` | 0 | 0 | `0` |
| `credit_xag` | `REAL` | 0 | 0 | `0` |
| `debit_xpt` | `REAL` | 0 | 0 | `0` |
| `credit_xpt` | `REAL` | 0 | 0 | `0` |
| `debit_xpd` | `REAL` | 0 | 0 | `0` |
| `credit_xpd` | `REAL` | 0 | 0 | `0` |
| `eur_delta` | `REAL` | 0 | 0 | `0` |
| `usd_delta` | `REAL` | 0 | 0 | `0` |
| `zar_delta` | `REAL` | 0 | 0 | `0` |
| `xau_delta` | `REAL` | 0 | 0 | `0` |
| `xag_delta` | `REAL` | 0 | 0 | `0` |
| `xpt_delta` | `REAL` | 0 | 0 | `0` |
| `xpd_delta` | `REAL` | 0 | 0 | `0` |
| `balance_eur` | `REAL` | 0 | 0 | `0` |
| `balance_usd` | `REAL` | 0 | 0 | `0` |
| `balance_zar` | `REAL` | 0 | 0 | `0` |
| `balance_xau` | `REAL` | 0 | 0 | `0` |
| `balance_xag` | `REAL` | 0 | 0 | `0` |
| `balance_xpt` | `REAL` | 0 | 0 | `0` |
| `balance_xpd` | `REAL` | 0 | 0 | `0` |
| `created_at` | `TIMESTAMP` | 0 | 0 | `CURRENT_TIMESTAMP` |
| `updated_at` | `TIMESTAMP` | 0 | 0 | `CURRENT_TIMESTAMP` |
| `fix_message` | `TEXT` | 0 | 0 | `` |
| `fix_trade_id` | `TEXT` | 0 | 0 | `` |
| `fix_clord_id` | `TEXT` | 0 | 0 | `` |
| `fix_exec_id` | `TEXT` | 0 | 0 | `` |
| `fix_account` | `TEXT` | 0 | 0 | `` |
| `fix_settlement_date` | `TEXT` | 0 | 0 | `` |
| `fix_transact_time` | `TEXT` | 0 | 0 | `` |
| `fix_report_type` | `TEXT` | 0 | 0 | `` |
| `trader_name` | `TEXT` | 0 | 0 | `` |
| `fix_trans_type` | `TEXT` | 0 | 0 | `` |
| `rest_trade_id` | `TEXT` | 0 | 0 | `` |
| `abs_trade_value` | `REAL` | 0 | 0 | `` |
| `account` | `TEXT` | 0 | 0 | `` |
| `account_base_currency` | `TEXT` | 0 | 0 | `` |
| `account_id` | `TEXT` | 0 | 0 | `` |
| `asset_class` | `TEXT` | 0 | 0 | `` |
| `contract_description` | `TEXT` | 0 | 0 | `` |
| `contract_size` | `REAL` | 0 | 0 | `` |
| `counter_currency` | `TEXT` | 0 | 0 | `` |
| `currency` | `TEXT` | 0 | 0 | `` |
| `currency_pair` | `TEXT` | 0 | 0 | `` |
| `last_px` | `REAL` | 0 | 0 | `` |
| `last_qty` | `REAL` | 0 | 0 | `` |
| `process_date` | `TEXT` | 0 | 0 | `` |
| `settlement_date_int` | `INTEGER` | 0 | 0 | `` |
| `settlement_price` | `REAL` | 0 | 0 | `` |
| `trade_currency` | `TEXT` | 0 | 0 | `` |
| `trade_date_int` | `INTEGER` | 0 | 0 | `` |
| `transact_time` | `TEXT` | 0 | 0 | `` |
| `source_system` | `TEXT` | 0 | 0 | `` |
| `fnc_number` | `TEXT` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_rest_trade_id` | No | `rest_trade_id` |
| `idx_fix_trade_id` | No | `fix_trade_id` |
| `idx_order_id` | No | `order_id` |
| `idx_side` | No | `side` |
| `idx_created_at` | No | `created_at` |
| `idx_doc_number` | No | `doc_number` |
| `idx_symbol` | No | `symbol` |
| `idx_trade_date` | No | `trade_date` |
| `sqlite_autoindex_trades_1` | Yes | `doc_number` |

### 7.2 `c:\Users\Joshua.Kress\Desktop\JupyterProject2\pmx_database.db`
Size: **13.18 MB**

| Table | Rows |
|---|---:|
| `account_opening_balances` | 3 |
| `sqlite_sequence` | 2 |
| `trades` | 149 |

#### Table `account_opening_balances`
Row count: **3**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `month` | `TEXT` | 1 | 0 | `` |
| `currency` | `TEXT` | 1 | 0 | `` |
| `opening_balance` | `REAL` | 1 | 0 | `0.0` |
| `updated_at` | `TEXT` | 1 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| `sqlite_autoindex_account_opening_balances_1` | Yes | `month`, `currency` |

#### Table `sqlite_sequence`
Row count: **2**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `name` | `` | 0 | 0 | `` |
| `seq` | `` | 0 | 0 | `` |

| Index | Unique | Columns |
|---|---:|---|
| _(none)_ | - | - |

#### Table `trades`
Row count: **149**

| Column | Type | Not Null | PK | Default |
|---|---|---:|---:|---|
| `id` | `INTEGER` | 0 | 1 | `` |
| `trade_date` | `TEXT` | 1 | 0 | `` |
| `value_date` | `TEXT` | 1 | 0 | `` |
| `symbol` | `TEXT` | 1 | 0 | `` |
| `side` | `TEXT` | 1 | 0 | `` |
| `narration` | `TEXT` | 0 | 0 | `` |
| `quantity` | `REAL` | 1 | 0 | `0` |
| `price` | `REAL` | 1 | 0 | `0` |
| `settle_currency` | `TEXT` | 1 | 0 | `''` |
| `settle_amount` | `REAL` | 1 | 0 | `0` |
| `doc_number` | `TEXT` | 0 | 0 | `` |
| `clord_id` | `TEXT` | 0 | 0 | `` |
| `order_id` | `TEXT` | 0 | 0 | `` |
| `fnc_number` | `TEXT` | 0 | 0 | `` |
| `debit_usd` | `REAL` | 0 | 0 | `0` |
| `credit_usd` | `REAL` | 0 | 0 | `0` |
| `debit_zar` | `REAL` | 0 | 0 | `0` |
| `credit_zar` | `REAL` | 0 | 0 | `0` |
| `debit_xau` | `REAL` | 0 | 0 | `0` |
| `credit_xau` | `REAL` | 0 | 0 | `0` |
| `balance_usd` | `REAL` | 0 | 0 | `0` |
| `balance_zar` | `REAL` | 0 | 0 | `0` |
| `balance_xau` | `REAL` | 0 | 0 | `0` |
| `rest_trade_id` | `TEXT` | 0 | 0 | `` |
| `account` | `TEXT` | 0 | 0 | `` |
| `counter_currency` | `TEXT` | 0 | 0 | `` |
| `currency` | `TEXT` | 0 | 0 | `` |
| `currency_pair` | `TEXT` | 0 | 0 | `` |
| `last_px` | `REAL` | 0 | 0 | `` |
| `last_qty` | `REAL` | 0 | 0 | `` |
| `process_date` | `TEXT` | 0 | 0 | `` |
| `trade_currency` | `TEXT` | 0 | 0 | `` |
| `transact_time` | `TEXT` | 0 | 0 | `` |
| `source_system` | `TEXT` | 0 | 0 | `` |
| `trader_name` | `TEXT` | 0 | 0 | `` |
| `raw_payload` | `TEXT` | 0 | 0 | `` |
| `created_at` | `TIMESTAMP` | 0 | 0 | `CURRENT_TIMESTAMP` |

| Index | Unique | Columns |
|---|---:|---|
| `idx_pmx_fnc_number` | No | `fnc_number` |
| `idx_pmx_doc_number` | No | `doc_number` |
| `idx_pmx_order_id` | No | `order_id` |
| `idx_pmx_trade_date` | No | `trade_date` |
| `idx_pmx_symbol` | No | `symbol` |
| `sqlite_autoindex_trades_1` | Yes | `doc_number` |

### 7.3 `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2_platform_clean.db`
Not present in workspace.

### 7.4 `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2-platform\server\fx_trading_ledger.db`
Size: **0.0 MB**

No tables found.

### 7.5 `c:\Users\Joshua.Kress\Desktop\JupyterProject2\j2-platform\server\pmx_database.db`
Size: **0.0 MB**

No tables found.

## 8. Data Integrity and Pipeline Behavior
| Control | Implementation |
|---|---|
| Schema bootstrap | `models/database.py::initialize_database()` |
| PMX DB management | `j2-platform/server/server.py` uses `PMX_DB_PATH` and helper accessors |
| Clean warehouse | `services/clean_data_pipeline.py` with batch metadata and row hashes |
| Hashing | SHA-256 over deterministic JSON payload per row |
| Cache invalidation | PMX/TradeMC sync endpoints clear heavy caches and trigger clean pipeline |
| Trade number normalization | Frontend + backend normalize to uppercase before persist |

## 9. GitHub Process and Comments (Repo Governance)
| File | Key Contents |
|---|---|
| `CONTRIBUTING.md` | Branch naming, commit standards, PR requirements, quality gates, secret handling |
| `CODEOWNERS` | Ownership defaults and path-level owners for backend/frontend |
| `.github/pull_request_template.md` | Structured PR comments: Summary, Scope, Validation, Risk/Rollback, Notes |
| `.github/workflows/ci.yml` | CI comments/checks: frontend build and backend py_compile smoke checks |

### 9.1 CI Workflow Snapshot
```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  frontend-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - name: Install and build frontend
        run: |
          npm --prefix j2-platform/client ci
          npm --prefix j2-platform/client run -s build

  backend-smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Python syntax smoke check
        run: |
          python -m py_compile j2-platform/server/server.py
          python -m py_compile services/trademc_service.py
          python -m py_compile services/rest_service.py
```

### 9.2 PR Template Snapshot
```md
## Summary
- What changed
- Why it changed

## Scope
- [ ] Backend
- [ ] Frontend
- [ ] Database / schema
- [ ] DevOps / CI

## Validation
- [ ] Backward compatibility checked
- [ ] Data integrity impact reviewed

## Risk and Rollback
- Risk level: Low / Medium / High
- Rollback plan:

## Notes
- Linked ticket / context:
```

### 9.3 Contributing Snapshot
```md
# Contributing Guide

## Branching
- Use short-lived branches from `main`.
- Branch naming:
  - `feature/<ticket-or-topic>`

## Commit Standards
- Use conventional-style commit messages:
- Keep commits focused and atomic.

## Pull Requests
- Link the business/issue context.
- Include testing evidence for backend and/or frontend changes.
- Note schema/data changes explicitly.
- Require at least one reviewer before merge.

## Data and Secrets
- Never commit `.env`, database files, exports, or generated reports.
- Keep all secrets in environment variables or secret managers.

## Quality Gate
- Frontend changes: `npm --prefix j2-platform/client run -s build`
- Backend changes: run unit/integration checks before merge.
```

### 9.4 CODEOWNERS Snapshot
```txt
# Default ownership
* @joshua-kress

# Backend
/j2-platform/server/ @joshua-kress
/services/ @joshua-kress
/models/ @joshua-kress

# Frontend
/j2-platform/client/ @joshua-kress
```

## 10. Key File Map
| Path | Role |
|---|---|
| `j2-platform/server/server.py` | Main API server, route handlers, analytics orchestration, auth/session logic |
| `models/database.py` | Ledger DB bootstrap and connection settings |
| `models/trade.py` | Trade CRUD, load/query helpers, manual trade backup/restore |
| `services/rest_service.py` | PMX/StoneX HTTP client functions |
| `services/trademc_service.py` | TradeMC sync and local persistence logic |
| `services/clean_data_pipeline.py` | Clean warehouse and integrity hash pipeline |
| `j2-platform/client/src/App.tsx` | UI modules/tabs and state management |
| `j2-platform/client/src/api/client.ts` | Typed API wrapper for frontend calls |

## 11. Operational Notes
- Runtime authoritative DBs are currently root-level `fx_trading_ledger.db` and `pmx_database.db`.
- `j2-platform/server/*.db` copies are present but empty in this workspace.
- `j2_platform_clean.db` is created when clean pipeline is executed.
- For production hardening, move hardcoded credentials/session defaults entirely to env/secret manager.
