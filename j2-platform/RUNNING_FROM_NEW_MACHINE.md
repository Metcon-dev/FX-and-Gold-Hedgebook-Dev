# Running J2 Platform On A New Machine

Use this checklist if you want to copy the app to a fresh machine and run it end to end.

## 1) System Requirements

- Windows 10 or Windows 11
- Python 3.10 or newer
- Node.js 18 or newer
- Git
- Microsoft Edge or Chromium-compatible browser
- Internet access for first-time package installation

## 2) Python Environment

Install the Python packages in [`server/requirements.txt`](server/requirements.txt).

Required Python packages:

- `flask`
- `flask-cors`
- `pandas`
- `openpyxl`
- `requests`
- `Office365-REST-Python-Client`
- `numpy`
- `yfinance`
- `scikit-learn`
- `reportlab`
- `fpdf2`
- `Pillow`
- `pytesseract`
- `playwright`

## 3) Node / Client Environment

In [`client/package.json`](client/package.json), the app requires:

- `react`
- `react-dom`
- `recharts`
- `html2canvas`
- `jspdf`
- `vite`
- `typescript`

Install dependencies with `npm install` in the `client` folder.

## 4) Browser Automation

The dashboard PDF builder uses Playwright.

You must also install a browser for Playwright on the new machine:

- `npx playwright install`

## 5) Database / Local Data Files

These files must exist for the app to work correctly:

- [`server/fx_trading_ledger.db`](server/fx_trading_ledger.db)
- [`server/pmx_database.db`](server/pmx_database.db)
- [`server/trademc.db`](server/trademc.db)
- [`server/piro_all_customers_orders_items_flat.csv`](server/piro_all_customers_orders_items_flat.csv)

Depending on your setup, the app may also read:

- project root `.env`
- [`server/.env`](server/.env) if present

## 6) Runtime Secrets / Environment Variables

You will usually need these configured in `.env`:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_FROM`
- `DAILY_TRADING_REPORT_EMAIL_TO`
- `DASHBOARD_REPORT_EMAIL_TO` if you still use the legacy path
- `APP_AUTH_USERNAME`
- `APP_AUTH_PASSWORD`
- `APP_AUTH_PBKDF2_ITERATIONS`
- `APP_AUTH_SESSION_SECONDS`
- `APP_AUTH_COOKIE_SECURE`
- `LEDGER_DB_PATH`
- `PMX_DB_PATH`
- `PMX_EXPORT_TRADES_DIR`
- `TRADE_DATA_START_DATE`
- `ENABLE_FISCAL_DATE_FILTER`
- `ENABLE_FISCAL_PURGE`

## 7) Files Needed To Run The App

Server:

- [`server/server.py`](server/server.py)
- [`server/dashboard_pdf_builder.py`](server/dashboard_pdf_builder.py)
- [`server/sharepoint_service.py`](server/sharepoint_service.py)
- [`server/requirements.txt`](server/requirements.txt)

Client:

- [`client/src/App.tsx`](client/src/App.tsx)
- [`client/src/index.css`](client/src/index.css)
- [`client/package.json`](client/package.json)
- [`client/vite.config.ts`](client/vite.config.ts)

## 8) Recommended Startup Order

1. Copy the repo to the new machine.
2. Copy the database files and any required `.env` files.
3. Create and activate a Python virtual environment.
4. Install Python dependencies from `server/requirements.txt`.
5. Run `npx playwright install`.
6. Run `npm install` in `client`.
7. Start the client and server.

## 9) Notes

- `client/node_modules` should be regenerated, not committed.
- The `.db` files are part of the working app state and should be backed up with the repo.
- If the database files are moved, update the `*_DB_PATH` environment variables accordingly.
