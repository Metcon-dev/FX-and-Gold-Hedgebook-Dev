# J2 Platform Runtime Files

This repository includes both source code and local runtime assets. The app expects the following files to be present in a working checkout.

## Required server files

- `server/server.py`
- `server/dashboard_pdf_builder.py`
- `server/sharepoint_service.py`
- `server/requirements.txt`
- `server/.env` or project root `.env` with local secrets and endpoint configuration

## Required local data files

- `server/fx_trading_ledger.db`
- `server/pmx_database.db`
- `server/trademc.db`
- `server/piro_all_customers_orders_items_flat.csv`

## Required report and support artifacts

- `server/valid_test_report.pdf`
- `server/start_test.txt`
- `server/_new_pdf_func.py`

## Client files

- `client/package.json`
- `client/src/App.tsx`
- `client/vite.config.ts`

## Notes

- The database files are part of the app’s local state and should be backed up with the repo.
- `client/node_modules` is not required in version control and is regenerated from `package.json`.
- If any of the database files are moved, the server’s environment variables may need to be updated to match.
