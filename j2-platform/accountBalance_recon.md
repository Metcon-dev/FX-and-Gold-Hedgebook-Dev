# Account Balance Recon Rebuild Spec (Open Positions Reval)

## Goal
Completely redesign the **Account Balances and Recon** section shown on the **Open Positions Reval** page so deltas are trustworthy, especially for **ZAR**.

This document gives your CLI agent full implementation context: where data is fetched, where reconciliation is computed, where UI consumes it, and what to replace.

---

## Current UI Entry Point
- File: `j2-platform/client/src/App.tsx`
- Component: `OpenPositionsReval()`
- Starts around: line ~1054

### Current API calls from this page
1. `GET /api/pmx/open-positions-reval`
- used in `load()` for open positions table + summary + market + embedded account balances

2. `GET /api/pmx/account-balances`
- called in background after open positions reval response to refresh balances

3. `GET /api/pmx/reconciliation`
- called by `loadReconSummary()` for XAU/USD/ZAR transaction-based recon summary + rows

4. `GET /api/pmx/ledger-full-csv`
- download path for full recon CSV

### UI fields shown in recon summary
- Starting Balance (XAU, USD, ZAR)
- Net Transactions
- Expected Balance
- Actual Balance
- Delta

Current UI delta fallback logic is implemented in `OpenPositionsReval()`:
- `delta = expected - actual` fallback when summary delta missing
- then row-level red highlight when abs(delta) > epsilon

---

## Current Backend Endpoints
- File: `j2-platform/server/server.py`

### Endpoints used by this section
- `GET /api/pmx/open-positions-reval` (line ~8360)
- `GET /api/pmx/account-balances` (line ~8371)
- `GET /api/pmx/account-recon` (line ~8377)
- `POST /api/pmx/account-recon/opening-balance` (line ~8388)
- `GET /api/pmx/account-recon/opening-balances` (line ~8419)

### Core backend functions involved
- `build_open_positions_reval(...)` (line ~4063)
- `_fetch_open_positions_account_balances(...)` (earlier in file, used by reval + recon)
- `_fetch_account_recon_transactions(...)` (line ~3506)
- `build_account_recon(...)` (line ~3638)

---

## Current Recon Algorithm (What exists today)
In `build_account_recon(...)`:
1. Determine `start_date` and `end_date` (defaults to month start -> today)
2. Fetch live balances via `_fetch_open_positions_account_balances`
3. Fetch statement movement totals via `_fetch_account_recon_transactions`
4. Load opening balances from table `account_opening_balances` by `month` + `currency`
5. For each currency in `(XAU, USD, ZAR)` compute:
- `expected = opening_balance + transaction_total`
- `delta = actual_balance - expected`

Returned payload:
- `start_date`, `end_date`, `month`
- `currencies: { XAU|USD|ZAR -> opening_balance, transaction_total, expected_balance, actual_balance, delta }`
- status flags: `actual_balances_ok`, `transactions_ok`, `error`

---

## Why ZAR delta is likely unreliable now
The current transaction fetch (`_fetch_account_recon_transactions`) does broad key-token matching and two statement calls:

1. Call A: `col1=LC`, `col2=GLD` (intended to gather USD + XAU)
2. Call B: `col1=ZAR`, `col2=None` (intended to gather ZAR)

Then `_accumulate_rows(...)` classifies values by checking key text tokens like:
- XAU: `XAU|GLD|GOLD|OZ|COL2`
- USD: `USD|LC|COL1`
- ZAR: `ZAR`

This is fragile and can cause:
- misclassification between USD/ZAR/LC columns
- accidental inclusion of non-movement fields
- inconsistent sign handling when DR/CR labels vary
- double counting or undercounting across view variants

---

## Required Rebuild Outcome
Replace the existing account recon implementation with a deterministic, auditable pipeline.

### Functional requirements
1. Reconciliation must support **XAU, USD, ZAR**.
2. Delta signs must be consistent and documented:
- use one canonical formula everywhere (`actual - expected` OR `expected - actual`) and keep UI/backend aligned.
3. ZAR recon must be sourced from explicit ZAR movement fields only (no heuristic cross-currency fallback).
4. Every included row must have traceability fields:
- source statement view
- parsed debit/credit inputs
- computed signed movement
- reason/include flag
5. Remove ambiguous token-based accumulation for recon totals.

---

## Recommended Implementation Plan

### Phase 1: Build canonical statement extraction layer
Create a single parser that:
- pulls PMX statement rows for required views
- normalizes each row into canonical schema:
  - `doc_number`, `trade_date`, `value_date`, `row_type`, `symbol`, `side`, `narration`
  - `movement_xau`, `movement_usd`, `movement_zar` (signed)
  - `source_view` (e.g. `USD/NONE`, `ZAR/NONE`, `LC/GLD`)
  - `parse_flags` and `parse_notes`

Hard rule: movement fields must come from explicit debit/credit balance columns or deterministic symbol+side math; do not infer by loose key token scanning.

### Phase 2: Build recon engine from canonical rows
For each currency:
- `transaction_total = sum(movement_currency where included=true)`
- `expected = opening + transaction_total`
- `delta = actual - expected` (or chosen canonical)

Also output diagnostics:
- `row_count_total`
- `row_count_included_by_currency`
- `excluded_reason_counts`
- `latest_row_timestamp`

### Phase 3: Replace `/api/pmx/account-recon`
- Keep route contract stable where possible
- Add `debug` section with row-level trace summaries
- Ensure response includes precise values and calculation mode metadata

### Phase 4: Align Open Positions Reval UI
In `OpenPositionsReval()`:
- consume new recon response directly
- remove fallback delta math that can conflict with backend
- display formula label near Delta (example: `Delta = Actual - Expected`)
- show recon warning banner only from backend-consistent delta flags

### Phase 5: Validation and tests
Add deterministic tests for:
1. pure DR/CR USD rows
2. pure DR/CR ZAR rows
3. XAU rows with OZ movements
4. mixed-day payload with both statement views
5. no rows / missing balances
6. sign consistency checks

---

## Data Storage Context
Opening balances are persisted in SQLite table:
- `account_opening_balances(month, currency, opening_balance, updated_at)`

Routes already exist to read/write these values:
- `POST /api/pmx/account-recon/opening-balance`
- `GET /api/pmx/account-recon/opening-balances`

Keep this model unless there is a strong migration reason.

---

## Caching Context
Recon responses are currently cached via heavy route cache key:
- prefix `account_recon`

When changing recon internals:
- ensure cache invalidation still works when opening balance updates
- include key versioning if response schema changes to avoid stale-client issues

---

## Acceptance Criteria
1. ZAR delta is reproducible from exported row-level movement trace.
2. Running recon twice with same inputs returns identical totals.
3. UI delta and backend delta always match (no fallback mismatch).
4. No heuristic token-only classification remains in final recon totals.
5. CSV/export totals reconcile to on-screen totals for each currency.

---

## Exact Files to Refactor
Primary:
- `j2-platform/server/server.py`
  - `_fetch_account_recon_transactions`
  - `build_account_recon`
  - `/api/pmx/account-recon` response construction

Secondary:
- `j2-platform/client/src/App.tsx`
  - `OpenPositionsReval()` recon state + rendering

Reference only:
- `j2-platform/client/src/api/client.ts`
- existing PMX reconciliation logic in server for shared parsing ideas

---

## Note for CLI Agent
Treat this as a full rewrite of the account recon pipeline, not an incremental patch. Keep endpoint names stable, but prioritize correctness and traceability over preserving legacy internals.
