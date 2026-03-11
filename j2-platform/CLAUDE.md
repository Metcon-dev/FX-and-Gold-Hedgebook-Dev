# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Backend (Flask, port 5001)
```bash
cd server
pip install -r requirements.txt
python server.py
```

### Frontend (React/Vite, port 5173)
```bash
cd client
npm install
npm run dev        # dev server with hot reload + proxy to :5001
npm run build      # production build → client/dist/
npm run preview    # preview production build
```

Both must run simultaneously. Vite proxies all `/api/*` requests to `http://localhost:5001` (10-minute timeout for heavy sync routes).

## Architecture Overview

### Stack
- **Backend**: Flask (Python) — single file `server/server.py` (~8500 lines)
- **Frontend**: React 18 + TypeScript — single component file `client/src/App.tsx` (~5900 lines)
- **Databases**: Two SQLite files in `server/`
  - `pmx_database.db` — PMX/StoneX trades, synced from external API
  - `fx_trading_ledger.db` — manual FX/gold trades managed via `models/` layer
- **API client**: `client/src/api/client.ts` — all `/api/*` calls centralised here

### Data Sources
The platform aggregates data from three sources:
1. **PMX/StoneX** — external REST API (gold/FX trading platform); synced into `pmx_database.db`
2. **TradeMC** — external REST API (client-side metals bookings); synced into `pmx_database.db`
3. **Manual trades** — stored in `fx_trading_ledger.db`, loaded via `models.trade`

### Server Structure (`server/server.py`)
- **`.env` loading** — called at startup; sets credentials and config via environment variables
- **Auth** — `@app.before_request` guard; PBKDF2-SHA256 passwords, JWT tokens in httpOnly cookies; roles: `read`, `write`, `admin`
- **Caching** — `_get_cached_heavy_result()` (TTL 20s) for expensive report builders; `TRADEMC_LIVE_PRICES_CACHE` (TTL 15s) for live price polling
- **Route groups**: `/api/auth/*`, `/api/pmx/*`, `/api/trademc/*`, `/api/trades/*`, `/api/hedging`, `/api/ticket/*`, `/api/weighted-average/*`, `/api/export-trades/*`, `/api/profit/*`
- **Report builders** — pure functions like `build_profit_monthly_report()` called by routes via the heavy cache wrapper
- **PMX integration** — session credentials cached with a threading lock; auto-login retry on expiry

### Key env vars (loaded from `.env`)
| Variable | Default | Purpose |
|---|---|---|
| `FLASK_SECRET_KEY` / `APP_AUTH_SECRET` | — | JWT signing |
| `APP_AUTH_SESSION_SECONDS` | 43200 | Session lifetime |
| `PMX_EXPORT_TRADES_DIR` | `T:\Platform Doc Testing` | Excel export output path |
| `FISCAL_TRADES_START_DATE` | 2026-03-01 | Report date floor |
| `STONEX_SUBSCRIPTION_KEY` | — | PMX API auth fallback |

### Frontend Structure (`client/src/App.tsx`)
All major UI lives in one file. Key patterns:
- **`usePersistentState(key, default)`** — localStorage-backed state; used for filters, active tab, cached recon data
- **Tab components**: `PMXLedger`, `TradeMCTrades`, `GoldHedging`, `OpenPositionsReval`, `ForwardExposure`, `SupplierBalances`, `WeightedAverage`, `TradingTicket`, `ExportTrades`, `XAUReconciliation`, `AccountBalances`, `ProfitTab`, `UserManagement`
- **`EditableTradeNum`** — inline editable cell component used to assign trade numbers to PMX rows
- **Auto-sync** — `window.setInterval` every 3 minutes fires `PMX_AUTO_SYNC_EVENT` and `TRADEMC_AUTO_SYNC_EVENT` custom events; components listen and refresh
- **Formatting helpers**: `fmt()`, `fmtDate()`, `fmtDateTime()`, `parseLooseNumber()` (handles `(5)` → `-5`, `1,000` → `1000`)
- **Number CSS class**: `numClass(val)` returns `"num positive"` / `"num negative"` / `"num"` for coloring

### Profit Report Logic (important domain detail)
`build_profit_monthly_report()` splits per-trade ZAR profit into:
- **`metal_profit_zar`** = `profit_usd × tm_wa_fx` (USD price spread at TradeMC WA FX rate)
- **`exchange_profit_zar`** = `profit_zar − metal_profit_zar` (residual from PMX FX rate differential)

`tm_wa_fx` is derived as `tm_side_zar / tm_side_usd` from TradeMC booking data. Falls back to `wa_usdzar` (PMX WA FX) if TradeMC lacks ZAR values. Total `profit_zar` is always `sell_side_zar − buy_side_zar` and is unaffected by the split.
