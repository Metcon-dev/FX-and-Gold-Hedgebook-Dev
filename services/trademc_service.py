"""
TradeMC Directus API Service
Handles fetching and storing trade data from the TradeMC Directus API.
"""
import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
import os
import time

# API Configuration (hardcoded)
TRADEMC_BASE_URL = "https://trademc-admin.metcon.co.za"
TRADEMC_API_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"


def _trademc_base_url() -> str:
    return TRADEMC_BASE_URL.strip().rstrip("/")


def _trademc_api_key() -> str:
    return TRADEMC_API_KEY.strip()

# Database path (same as main trading db)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'fx_trading_ledger.db')


def get_api_headers() -> Dict[str, str]:
    """Get headers for TradeMC API requests."""
    api_key = _trademc_api_key()
    if not api_key:
        raise RuntimeError("TRADEMC_API_KEY hardcoded value is empty.")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _to_float(value: Any) -> Optional[float]:
    """Best-effort numeric parser that returns None for blanks/invalid values."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_utc_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO timestamp (including trailing Z) into a UTC-aware datetime."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_utc_iso(value: datetime) -> str:
    """Format UTC datetime to Directus-friendly ISO string with millisecond precision."""
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _subtract_seconds_from_iso(value: Optional[str], seconds: int) -> Optional[str]:
    """Return ISO timestamp shifted backwards by `seconds`; None when parsing fails."""
    parsed = _parse_utc_iso(value)
    if parsed is None:
        return value
    shifted = parsed - timedelta(seconds=max(0, int(seconds)))
    return _format_utc_iso(shifted)


_TRADE_COMPARE_FIELDS: List[str] = [
    "id",
    "status",
    "company_id",
    "weight",
    "notes",
    "ref_number",
    "trade_timestamp",
    "zar_per_troy_ounce",
    "zar_to_usd",
    "requested_zar_per_gram",
    "zar_per_troy_ounce_confirmed",
    "zar_to_usd_confirmed",
    "usd_per_troy_ounce_confirmed",
    "date_created",
    "date_updated",
    "evo_exported",
]

_TRADE_COMPARE_NUMERIC_FIELDS = {
    "weight",
    "zar_per_troy_ounce",
    "zar_to_usd",
    "requested_zar_per_gram",
    "zar_per_troy_ounce_confirmed",
    "zar_to_usd_confirmed",
    "usd_per_troy_ounce_confirmed",
}


def _norm_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _norm_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _norm_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _normalize_trade_compare_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize TradeMC trade record values for deterministic remote/local comparisons."""
    out: Dict[str, Any] = {}
    for field in _TRADE_COMPARE_FIELDS:
        raw = row.get(field)
        if field == "id":
            out[field] = _norm_int(raw)
        elif field in {"company_id"}:
            out[field] = _norm_int(raw)
        elif field in _TRADE_COMPARE_NUMERIC_FIELDS:
            out[field] = _norm_float(raw)
        elif field == "evo_exported":
            out[field] = bool(raw)
        else:
            out[field] = _norm_text(raw)
    return out


def _verify_full_replace_sync(expected_by_id: Dict[int, Dict[str, Any]], synced_at: str) -> Dict[str, Any]:
    """Verify that local rows written in a full replace match the remote snapshot used for the write."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, status, company_id, weight, notes, ref_number,
               trade_timestamp, zar_per_troy_ounce, zar_to_usd,
               requested_zar_per_gram, zar_per_troy_ounce_confirmed,
               zar_to_usd_confirmed, usd_per_troy_ounce_confirmed,
               date_created, date_updated, evo_exported
        FROM trademc_trades
        WHERE last_synced = ?
        ORDER BY id
        """,
        (synced_at,),
    )
    rows = cursor.fetchall()
    conn.close()

    local_by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        raw = dict(row)
        trade_id = _norm_int(raw.get("id"))
        if trade_id is None:
            continue
        local_by_id[trade_id] = _normalize_trade_compare_record(raw)

    expected_ids = set(expected_by_id.keys())
    local_ids = set(local_by_id.keys())
    missing_ids = sorted(expected_ids - local_ids)
    extra_ids = sorted(local_ids - expected_ids)

    field_mismatch_ids: List[int] = []
    sample_diffs: List[Dict[str, Any]] = []
    for trade_id in sorted(expected_ids & local_ids):
        exp_row = expected_by_id.get(trade_id) or {}
        loc_row = local_by_id.get(trade_id) or {}
        if exp_row == loc_row:
            continue
        field_mismatch_ids.append(trade_id)
        if len(sample_diffs) < 5:
            changed = {}
            for field in _TRADE_COMPARE_FIELDS:
                if exp_row.get(field) != loc_row.get(field):
                    changed[field] = {
                        "remote": exp_row.get(field),
                        "local": loc_row.get(field),
                    }
            sample_diffs.append({"id": trade_id, "changed_fields": changed})

    ok = not missing_ids and not extra_ids and not field_mismatch_ids
    return {
        "ok": ok,
        "written_count": len(local_by_id),
        "expected_count": len(expected_by_id),
        "missing_ids": missing_ids[:20],
        "extra_ids": extra_ids[:20],
        "field_mismatch_ids": field_mismatch_ids[:20],
        "sample_diffs": sample_diffs,
    }


def _load_local_trade_compare_rows_by_ids(trade_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Load comparable local trade snapshots for the provided IDs."""
    if not trade_ids:
        return {}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    out: Dict[int, Dict[str, Any]] = {}
    chunk_size = 900
    select_cols = ", ".join(_TRADE_COMPARE_FIELDS)

    try:
        for i in range(0, len(trade_ids), chunk_size):
            chunk = trade_ids[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            cursor.execute(
                f"SELECT {select_cols} FROM trademc_trades WHERE id IN ({placeholders})",
                chunk,
            )
            for row in cursor.fetchall():
                raw = dict(row)
                trade_id = _norm_int(raw.get("id"))
                if trade_id is None:
                    continue
                out[trade_id] = _normalize_trade_compare_record(raw)
    finally:
        conn.close()

    return out


def fetch_trademc_historic_data(
    limit: int = 200,
    fields: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch market history rows from TradeMC Directus historic_data collection.

    Args:
        limit: Number of rows to fetch.
        fields: Optional list of fields to request.

    Returns:
        dict: API response with `data` list, or None when request fails.
    """
    url = f"{_trademc_base_url()}/items/historic_data"
    params: Dict[str, Any] = {
        "limit": max(1, int(limit)),
        "sort": "-timestamp,-id",
    }
    if fields:
        params["fields"] = ",".join(fields)

    try:
        response = requests.get(url, headers=get_api_headers(), params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        print(f"API Error: {response.status_code} - {response.text}")
        return None
    except requests.exceptions.RequestException as exc:
        print(f"Request failed: {exc}")
        return None


def get_latest_trademc_market_prices(sample_size: int = 200) -> Dict[str, Any]:
    """
    Get latest usable USD/ZAR and XAU/USD prices from TradeMC historic_data.

    Notes:
        - USD/ZAR uses `zar_to_usd` (fallback `zar_to_usd_ask`).
        - XAU/USD uses `usd_per_troy_ounce` (fallback `usd_per_troy_ounce_ask`).
        - If XAU/USD is missing, derive from `zar_per_troy_ounce / zar_to_usd`.
    """
    fields = [
        "id",
        "timestamp",
        "zar_per_troy_ounce",
        "zar_to_usd",
        "usd_per_troy_ounce",
        "zar_to_usd_ask",
        "usd_per_troy_ounce_ask",
    ]
    payload = fetch_trademc_historic_data(limit=sample_size, fields=fields)
    if not payload or not isinstance(payload, dict):
        return {"ok": False, "error": "Failed to fetch TradeMC historic_data"}

    rows_raw = payload.get("data")
    rows = rows_raw if isinstance(rows_raw, list) else []
    if not rows:
        return {"ok": False, "error": "TradeMC historic_data returned no rows"}

    def _row_sort_key(row: Dict[str, Any]):
        ts = str(row.get("timestamp") or "")
        rid = row.get("id")
        try:
            rid_num = int(rid)
        except Exception:
            rid_num = 0
        return (ts, rid_num)

    rows_sorted = sorted(rows, key=_row_sort_key, reverse=True)
    newest = rows_sorted[0] if rows_sorted else {}

    usd_zar: Optional[float] = None
    usd_zar_source = ""
    xau_usd: Optional[float] = None
    xau_usd_source = ""

    for row in rows_sorted:
        if usd_zar is None:
            fx = _to_float(row.get("zar_to_usd"))
            fx_ask = _to_float(row.get("zar_to_usd_ask"))
            zar_oz = _to_float(row.get("zar_per_troy_ounce"))
            usd_oz = _to_float(row.get("usd_per_troy_ounce"))
            usd_oz_ask = _to_float(row.get("usd_per_troy_ounce_ask"))

            # Prefer higher-resolution implied FX from the same market tick.
            if zar_oz is not None and usd_oz is not None and abs(usd_oz) > 1e-12:
                usd_zar = zar_oz / usd_oz
                usd_zar_source = "derived:zar_per_troy_ounce/usd_per_troy_ounce"
            elif zar_oz is not None and usd_oz_ask is not None and abs(usd_oz_ask) > 1e-12:
                usd_zar = zar_oz / usd_oz_ask
                usd_zar_source = "derived:zar_per_troy_ounce/usd_per_troy_ounce_ask"
            elif fx is not None and abs(fx) > 1e-12:
                usd_zar = fx
                usd_zar_source = "zar_to_usd"
            elif fx_ask is not None and abs(fx_ask) > 1e-12:
                usd_zar = fx_ask
                usd_zar_source = "zar_to_usd_ask"

        if xau_usd is None:
            xau = _to_float(row.get("usd_per_troy_ounce"))
            xau_ask = _to_float(row.get("usd_per_troy_ounce_ask"))
            if xau is not None and abs(xau) > 1e-12:
                xau_usd = xau
                xau_usd_source = "usd_per_troy_ounce"
            elif xau_ask is not None and abs(xau_ask) > 1e-12:
                xau_usd = xau_ask
                xau_usd_source = "usd_per_troy_ounce_ask"
            else:
                zar_oz = _to_float(row.get("zar_per_troy_ounce"))
                fx_local = _to_float(row.get("zar_to_usd"))
                if fx_local is None or abs(fx_local) <= 1e-12:
                    fx_local = _to_float(row.get("zar_to_usd_ask"))
                if (
                    zar_oz is not None
                    and fx_local is not None
                    and abs(fx_local) > 1e-12
                ):
                    xau_usd = zar_oz / fx_local
                    xau_usd_source = "derived:zar_per_troy_ounce/zar_to_usd"

        if usd_zar is not None and xau_usd is not None:
            break

    ok = usd_zar is not None or xau_usd is not None
    out: Dict[str, Any] = {
        "ok": ok,
        "timestamp": newest.get("timestamp"),
        "usd_zar": usd_zar,
        "xau_usd": xau_usd,
        "usd_zar_source": usd_zar_source or None,
        "xau_usd_source": xau_usd_source or None,
        "fetched_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    if not ok:
        out["error"] = "No usable USD/ZAR or XAU/USD values found in historic_data"
    return out


def initialize_trademc_table():
    """Create the TradeMC tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create trades table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trademc_trades (
            id INTEGER PRIMARY KEY,
            status TEXT,
            company_id INTEGER,
            weight REAL,
            notes TEXT,
            ref_number TEXT,
            trade_timestamp DATETIME,
            zar_per_troy_ounce REAL,
            zar_to_usd REAL,
            requested_zar_per_gram REAL,
            zar_per_troy_ounce_confirmed REAL,
            zar_to_usd_confirmed REAL,
            usd_per_troy_ounce_confirmed REAL,
            date_created DATETIME,
            date_updated DATETIME,
            evo_exported INTEGER DEFAULT 0,
            last_synced DATETIME
        )
    ''')
    
    # Create companies table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trademc_companies (
            id INTEGER PRIMARY KEY,
            status TEXT,
            company_name TEXT,
            registration_number TEXT,
            contact_number TEXT,
            email_address TEXT,
            sharepoint_identifier TEXT,
            trade_limit REAL,
            blocked INTEGER DEFAULT 0,
            vat_number TEXT,
            EVO_customer_code TEXT,
            refining_rate REAL,
            date_created DATETIME,
            date_updated DATETIME,
            last_synced DATETIME
        )
    ''')

    # Create weight transaction ledger table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trademc_weight_transactions (
            id INTEGER PRIMARY KEY,
            user_created TEXT,
            date_created DATETIME,
            user_updated TEXT,
            date_updated DATETIME,
            company_id INTEGER,
            pc_code TEXT,
            notes TEXT,
            type TEXT,
            weight REAL,
            rolling_balance REAL,
            transaction_timestamp DATETIME,
            gold_percentage REAL,
            old_id INTEGER,
            reversal_id INTEGER,
            trade_id INTEGER,
            last_synced DATETIME
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_status ON trademc_trades(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_notes ON trademc_trades(notes)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_timestamp ON trademc_trades(trade_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company ON trademc_trades(company_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_company_name ON trademc_companies(company_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_company ON trademc_weight_transactions(company_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_timestamp ON trademc_weight_transactions(transaction_timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_type ON trademc_weight_transactions(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_trademc_weight_trade ON trademc_weight_transactions(trade_id)')
    
    conn.commit()
    conn.close()



def fetch_trademc_trades(
    limit: int = 100,
    offset: int = 0,
    fields: Optional[List[str]] = None,
    min_trade_id: Optional[int] = None,
    min_date_updated: Optional[str] = None,
    sort: str = "id",
    include_total_count: bool = False,
    retry_attempts: int = 4,
) -> Optional[Dict]:
    """
    Fetch trades from TradeMC Directus API.
    
    Args:
        limit: Number of items to retrieve per request
        offset: Offset for pagination
        fields: List of fields to return (optional)
        min_trade_id: Only fetch trades with id greater than this value
        min_date_updated: Only fetch trades with date_updated > this ISO timestamp
        include_total_count: Ask Directus to include `meta.total_count`
    
    Returns:
        dict: API response containing trade data, or None on error
    """
    url = f"{_trademc_base_url()}/items/trade"
    
    base_params = {
        "limit": limit,
        "offset": offset,
        "sort": sort or "id",
    }
    
    if fields:
        base_params["fields"] = ",".join(fields)

    if min_trade_id is not None:
        base_params["filter[id][_gt]"] = min_trade_id

    if min_date_updated:
        base_params["filter[date_updated][_gte]"] = str(min_date_updated).strip()

    if include_total_count:
        base_params["meta"] = "total_count"

    max_attempts = max(1, int(retry_attempts or 1))
    transient_statuses = {304, 408, 425, 429, 500, 502, 503, 504}
    last_error = ""
    for attempt in range(max_attempts):
        params = dict(base_params)
        # Cache-bust each retry attempt to avoid stale intermediary responses.
        params["_cb"] = int(time.time() * 1000) + attempt
        try:
            response = requests.get(url, headers=get_api_headers(), params=params, timeout=45)
        except requests.exceptions.RequestException as e:
            last_error = f"Request failed: {e}"
            print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
            if attempt < max_attempts - 1:
                time.sleep(min(8, 0.75 * (2 ** attempt)))
                continue
            return None
        except OSError as e:
            last_error = f"OS error during TradeMC request: {e}"
            print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
            if attempt < max_attempts - 1:
                time.sleep(min(8, 0.75 * (2 ** attempt)))
                continue
            return None
        except Exception as e:
            last_error = f"Unexpected error during TradeMC request: {e}"
            print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
            if attempt < max_attempts - 1:
                time.sleep(min(8, 0.75 * (2 ** attempt)))
                continue
            return None

        if response.status_code == 200:
            try:
                result = response.json()
            except ValueError as exc:
                last_error = f"Invalid JSON response: {exc}"
                print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
                if attempt < max_attempts - 1:
                    time.sleep(min(8, 0.75 * (2 ** attempt)))
                    continue
                return None

            if not isinstance(result, dict) or "data" not in result:
                last_error = "Malformed JSON payload (missing data field)"
                print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
                if attempt < max_attempts - 1:
                    time.sleep(min(8, 0.75 * (2 ** attempt)))
                    continue
                return None

            data_count = len(result.get('data', [])) if isinstance(result, dict) else 0
            print(f"[FETCH] offset={params.get('offset')}, limit={params.get('limit')}, got {data_count} rows")
            return result

        snippet = str(response.text or "")[:200].replace("\n", " ").strip()
        last_error = f"API Error {response.status_code}: {snippet}"
        print(f"[FETCH] {last_error} (attempt {attempt + 1}/{max_attempts})")
        if response.status_code in transient_statuses and attempt < max_attempts - 1:
            time.sleep(min(8, 0.75 * (2 ** attempt)))
            continue
        return None

    if last_error:
        print(f"[FETCH] Failed after retries: {last_error}")
    return None


def update_trademc_trade_ref_number(trade_id: int, ref_number: str) -> Dict[str, Any]:
    """
    Update a TradeMC trade's ref_number on the remote Directus API and mirror locally.

    Args:
        trade_id: TradeMC trade ID.
        ref_number: New reference number (blank allowed).

    Returns:
        dict: Operation status and updated row details when available.
    """
    trade_id_int = int(trade_id or 0)
    if trade_id_int <= 0:
        return {"success": False, "error": "Invalid trade_id", "status": 400}

    normalized_ref = str(ref_number or "").strip()
    headers = get_api_headers()
    by_id_url = f"{_trademc_base_url()}/items/trade/{trade_id_int}"
    bulk_url = f"{_trademc_base_url()}/items/trade"

    response_data: Dict[str, Any] = {}
    patch_status: Optional[int] = None
    patch_details: str = ""
    patch_mode: str = ""

    def _patch_json(url: str, payload: Dict[str, Any], attempts: int = 3):
        last_err = ""
        max_attempts = max(1, int(attempts or 1))
        for attempt in range(max_attempts):
            try:
                res = requests.patch(url, headers=headers, json=payload, timeout=30)
                return res, ""
            except requests.exceptions.RequestException as exc:
                last_err = f"request failed: {exc}"
            except OSError as exc:
                last_err = f"os error: {exc}"
            except Exception as exc:
                last_err = f"unexpected error: {exc}"
            if attempt < max_attempts - 1:
                time.sleep(min(2.0, 0.4 * (2 ** attempt)))
        return None, last_err

    # Primary path: standard Directus item update.
    try:
        res, patch_err = _patch_json(by_id_url, {"ref_number": normalized_ref}, attempts=3)
        if res is None:
            patch_details = patch_err or "no response"
        else:
            patch_status = int(res.status_code)
            if res.status_code in (200, 201):
                parsed = res.json() if res.text else {}
                if isinstance(parsed, dict):
                    data = parsed.get("data")
                    if isinstance(data, dict):
                        response_data = data
                    elif isinstance(data, list) and data and isinstance(data[0], dict):
                        response_data = data[0]
                patch_mode = "item"
            else:
                patch_details = str(res.text or "").strip()[:500]
    except Exception as exc:
        patch_details = f"TradeMC request failed: {exc}"

    # Fallback path: bulk update endpoint for token scopes that block item route.
    if not response_data:
        try:
            bulk_payload = {"keys": [trade_id_int], "data": {"ref_number": normalized_ref}}
            res, bulk_err = _patch_json(bulk_url, bulk_payload, attempts=3)
            if res is None:
                patch_details = f"{patch_details} | bulk {bulk_err or 'no response'}".strip(" |")
            else:
                patch_status = int(res.status_code)
                if res.status_code in (200, 201):
                    parsed = res.json() if res.text else {}
                    if isinstance(parsed, dict):
                        data = parsed.get("data")
                        if isinstance(data, dict):
                            response_data = data
                        elif isinstance(data, list) and data and isinstance(data[0], dict):
                            response_data = data[0]
                    patch_mode = "bulk"
                else:
                    bulk_details = str(res.text or "").strip()[:500]
                    patch_details = f"{patch_details} | bulk: {bulk_details}".strip(" |")
        except Exception as exc:
            patch_details = f"{patch_details} | bulk request failed: {exc}".strip(" |")

    if not response_data:
        return {
            "success": False,
            "error": f"TradeMC update failed ({patch_status if patch_status is not None else 'no_response'})",
            "status": patch_status or 502,
            "details": patch_details or "No response payload from TradeMC update endpoint.",
        }

    # Always read back by ID-filter to confirm persisted remote state.
    remote_trade: Dict[str, Any] = {}
    visibility = fetch_trademc_trade_by_id(trade_id_int)
    if bool(visibility.get("ok")) and bool(visibility.get("found")):
        trade_payload = visibility.get("trade")
        if isinstance(trade_payload, dict):
            remote_trade = trade_payload
    if not remote_trade:
        remote_trade = response_data

    synced_at = datetime.now().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO trademc_trades (
            id, status, company_id, weight, notes, ref_number,
            trade_timestamp, zar_per_troy_ounce, zar_to_usd,
            requested_zar_per_gram, zar_per_troy_ounce_confirmed,
            zar_to_usd_confirmed, usd_per_troy_ounce_confirmed,
            date_created, date_updated, evo_exported, last_synced
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            status = excluded.status,
            company_id = excluded.company_id,
            weight = excluded.weight,
            notes = excluded.notes,
            ref_number = excluded.ref_number,
            trade_timestamp = excluded.trade_timestamp,
            zar_per_troy_ounce = excluded.zar_per_troy_ounce,
            zar_to_usd = excluded.zar_to_usd,
            requested_zar_per_gram = excluded.requested_zar_per_gram,
            zar_per_troy_ounce_confirmed = excluded.zar_per_troy_ounce_confirmed,
            zar_to_usd_confirmed = excluded.zar_to_usd_confirmed,
            usd_per_troy_ounce_confirmed = excluded.usd_per_troy_ounce_confirmed,
            date_created = excluded.date_created,
            date_updated = excluded.date_updated,
            evo_exported = excluded.evo_exported,
            last_synced = excluded.last_synced
        """,
        (
            trade_id_int,
            remote_trade.get("status"),
            remote_trade.get("company_id"),
            remote_trade.get("weight"),
            remote_trade.get("notes"),
            remote_trade.get("ref_number"),
            remote_trade.get("trade_timestamp"),
            remote_trade.get("zar_per_troy_ounce"),
            remote_trade.get("zar_to_usd"),
            remote_trade.get("requested_zar_per_gram"),
            remote_trade.get("zar_per_troy_ounce_confirmed"),
            remote_trade.get("zar_to_usd_confirmed"),
            remote_trade.get("usd_per_troy_ounce_confirmed"),
            remote_trade.get("date_created"),
            remote_trade.get("date_updated"),
            1 if bool(remote_trade.get("evo_exported")) else 0,
            synced_at,
        ),
    )
    conn.commit()
    conn.close()

    remote_ref = str(remote_trade.get("ref_number") or "")
    return {
        "success": True,
        "trade_id": trade_id_int,
        "ref_number": remote_ref,
        "remote": remote_trade,
        "synced_at": synced_at,
        "patch_mode": patch_mode or "unknown",
    }


def get_latest_local_trademc_trade_id() -> Optional[int]:
    """Get the latest TradeMC trade ID stored in the local database."""
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM trademc_trades")
    latest = cursor.fetchone()[0]
    conn.close()
    return int(latest) if latest is not None else None


def get_latest_local_trademc_date_updated() -> Optional[str]:
    """Get latest non-empty TradeMC date_updated from local cache."""
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MAX(date_updated)
        FROM trademc_trades
        WHERE date_updated IS NOT NULL AND TRIM(date_updated) <> ''
        """
    )
    latest = cursor.fetchone()[0]
    conn.close()
    txt = str(latest or "").strip()
    return txt or None


def get_local_trademc_snapshot_stats() -> Dict[str, Any]:
    """Return local TradeMC cache stats for diagnostics/safety checks."""
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*), MAX(id), MAX(date_updated)
        FROM trademc_trades
        """
    )
    count_val, max_id_val, max_date_val = cursor.fetchone()
    conn.close()
    return {
        "count": int(count_val or 0),
        "max_id": int(max_id_val) if max_id_val is not None else None,
        "max_date_updated": str(max_date_val or "").strip() or None,
    }


def get_remote_trademc_snapshot_stats() -> Dict[str, Any]:
    """Return remote TradeMC visibility stats from Directus."""
    response = fetch_trademc_trades(
        limit=1,
        offset=0,
        fields=["id", "date_updated"],
        sort="-id",
        include_total_count=True,
    )
    if not isinstance(response, dict):
        return {"ok": False, "error": "Failed to fetch remote snapshot stats"}

    rows = response.get("data") if isinstance(response.get("data"), list) else []
    max_id = None
    max_date_updated = None
    if rows:
        max_id_raw = rows[0].get("id")
        max_date_raw = rows[0].get("date_updated")
        try:
            max_id = int(max_id_raw) if max_id_raw is not None else None
        except (TypeError, ValueError):
            max_id = None
        max_date_updated = str(max_date_raw or "").strip() or None

    total_count_raw = (response.get("meta") or {}).get("total_count")
    try:
        total_count = int(total_count_raw) if total_count_raw is not None else None
    except (TypeError, ValueError):
        total_count = None

    return {
        "ok": True,
        "total_count": total_count,
        "max_id": max_id,
        "max_date_updated": max_date_updated,
    }


def fetch_trademc_trade_by_id(trade_id: int) -> Dict[str, Any]:
    """Fetch one trade by ID using filter query (works with token-scoped permissions)."""
    trade_id_int = int(trade_id or 0)
    if trade_id_int <= 0:
        return {"ok": False, "error": "Invalid trade_id"}

    # Use explicit ID filter because Directus item-by-id endpoint may be token-restricted.
    url = f"{_trademc_base_url()}/items/trade"
    params = {
        "limit": 1,
        "filter[id][_eq]": trade_id_int,
        "_cb": int(time.time() * 1000),
    }
    try:
        res = requests.get(url, headers=get_api_headers(), params=params, timeout=30)
        if res.status_code != 200:
            return {"ok": False, "error": f"Directus HTTP {res.status_code}", "details": str(res.text or "")[:500]}
        payload = res.json() if res.text else {}
    except requests.exceptions.RequestException as exc:
        return {"ok": False, "error": f"Request failed: {exc}"}
    except Exception as exc:
        return {"ok": False, "error": f"Failed to parse Directus response: {exc}"}

    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return {"ok": False, "error": "Unexpected Directus response"}
    if not rows:
        return {"ok": True, "found": False, "trade_id": trade_id_int}
    return {"ok": True, "found": True, "trade_id": trade_id_int, "trade": rows[0]}


def fetch_all_trademc_trades(
    progress_callback=None,
    min_trade_id: Optional[int] = None,
    min_date_updated: Optional[str] = None,
    page_size: int = 100,
    sort: str = "id",
    max_records: Optional[int] = 200000,
) -> Optional[List[Dict]]:
    """
    Fetch TradeMC trades from API using pagination.
    
    Args:
        progress_callback: Optional callback function(fetched, total) for progress updates
        min_trade_id: Only fetch trades where ID is greater than this value
        min_date_updated: Only fetch trades where date_updated > this value
        page_size: Number of trades to fetch per API request
        max_records: Optional hard ceiling for fetched rows. None means no ceiling.
    
    Returns:
        list: Trade records from the API, or None when an API error occurs
    """
    all_trades: List[Dict] = []
    limit = max(1, int(page_size))
    min_limit = 50
    total_count: Optional[int] = None

    print(f"[FETCH_ALL] Starting: min_id={min_trade_id}, min_date={min_date_updated}, page_size={page_size}")

    # For full snapshots (no date filter), keyset pagination is safer than offset pagination.
    if not min_date_updated:
        last_id = int(min_trade_id or 0)
        first_page = True
        current_limit = limit
        while True:
            result = fetch_trademc_trades(
                limit=current_limit,
                offset=0,
                min_trade_id=last_id if last_id > 0 else None,
                min_date_updated=None,
                sort="id",
                include_total_count=first_page,
            )
            if result is None:
                if current_limit > min_limit:
                    next_limit = max(min_limit, current_limit // 2)
                    print(
                        f"[FETCH_ALL] Page fetch failed after id={last_id}; "
                        f"reducing page size {current_limit} -> {next_limit} and retrying."
                    )
                    current_limit = next_limit
                    continue
                print(f"[FETCH_ALL] fetch_trademc_trades returned None for keyset after id={last_id}")
                return None
            if not isinstance(result, dict) or "data" not in result:
                print(f"[FETCH_ALL] No 'data' key in keyset result after id={last_id}. Keys={list(result.keys()) if isinstance(result, dict) else type(result)}")
                return None

            if first_page:
                first_page = False
                total_count_raw = (result.get("meta") or {}).get("total_count")
                try:
                    total_count = int(total_count_raw) if total_count_raw is not None else None
                except (TypeError, ValueError):
                    total_count = None
                print(f"[FETCH_ALL] Total count from API: {total_count}")

            trades = result.get("data") or []
            if not trades:
                print(f"[FETCH_ALL] Empty keyset page after id={last_id}, stopping. Total fetched: {len(all_trades)}")
                break

            all_trades.extend(trades)

            if progress_callback and total_count is not None:
                progress_callback(len(all_trades), total_count)

            ids = [int(t.get("id")) for t in trades if t.get("id") is not None]
            if not ids:
                print("[FETCH_ALL] Warning: received trades without IDs; stopping to avoid loop.")
                break
            last_id = max(ids)

            if max_records is not None and len(all_trades) >= int(max_records):
                print(f"[FETCH_ALL] Warning: Stopping at max_records={int(max_records)}")
                all_trades = all_trades[: int(max_records)]
                break

            if len(trades) < current_limit:
                print(f"[FETCH_ALL] Last keyset page ({len(trades)} < {current_limit}). Total fetched: {len(all_trades)}")
                break

        print(f"[FETCH_ALL] Done. Total trades fetched: {len(all_trades)}")
        return all_trades

    # Fallback: date-filtered incremental fetch still uses offset pagination.
    offset = 0
    current_limit = limit
    while True:
        result = fetch_trademc_trades(
            limit=current_limit,
            offset=offset,
            min_trade_id=min_trade_id,
            min_date_updated=min_date_updated,
            sort=sort,
            include_total_count=(offset == 0),
        )

        if result is None:
            if current_limit > min_limit:
                next_limit = max(min_limit, current_limit // 2)
                print(
                    f"[FETCH_ALL] Incremental page fetch failed at offset={offset}; "
                    f"reducing page size {current_limit} -> {next_limit} and retrying."
                )
                current_limit = next_limit
                continue
            print(f"[FETCH_ALL] fetch_trademc_trades returned None at offset={offset}")
            return None

        if not isinstance(result, dict) or "data" not in result:
            print(f"[FETCH_ALL] No 'data' key in result at offset={offset}. Keys={list(result.keys()) if isinstance(result, dict) else type(result)}")
            return None

        if offset == 0:
            total_count_raw = (result.get("meta") or {}).get("total_count")
            try:
                total_count = int(total_count_raw) if total_count_raw is not None else None
            except (TypeError, ValueError):
                total_count = None
            print(f"[FETCH_ALL] Total count from API: {total_count}")

        trades = result.get("data") or []
        if not trades:
            print(f"[FETCH_ALL] Empty page at offset={offset}, stopping. Total fetched: {len(all_trades)}")
            break

        all_trades.extend(trades)
        offset += len(trades)

        if progress_callback and total_count is not None:
            progress_callback(len(all_trades), total_count)

        if max_records is not None and len(all_trades) >= int(max_records):
            print(f"[FETCH_ALL] Warning: Stopping at max_records={int(max_records)}")
            all_trades = all_trades[: int(max_records)]
            break

        if len(trades) < current_limit:
            print(f"[FETCH_ALL] Last page ({len(trades)} < {current_limit}). Total fetched: {len(all_trades)}")
            break

    print(f"[FETCH_ALL] Done. Total trades fetched: {len(all_trades)}")
    return all_trades


def sync_trademc_trades(
    progress_callback=None,
    incremental: bool = False,
    prune_missing: bool = False,
    _verify_retry: int = 0,
) -> Dict[str, Any]:
    """
    Sync TradeMC trades from API to local database.
    
    Args:
        progress_callback: Optional callback function for progress updates
        incremental: When True, fetch only new IDs and remotely updated rows by date_updated.
            Default is False so each sync re-checks all remote trades for changed details.
        prune_missing: When True (full sync only), remove local rows no longer present remotely.
    
    Returns:
        dict: Sync result with counts
    """
    # Ensure table exists
    initialize_trademc_table()

    local_snapshot_before = get_local_trademc_snapshot_stats()
    remote_snapshot = get_remote_trademc_snapshot_stats()
    warnings: List[str] = []
    remote_total_count = None
    remote_max_id = None
    if not bool(remote_snapshot.get("ok")):
        warnings.append(str(remote_snapshot.get("error", "Unable to read remote TradeMC snapshot stats")))
    else:
        remote_total_count = remote_snapshot.get("total_count")
        remote_max_id = remote_snapshot.get("max_id")
        local_max_before = local_snapshot_before.get("max_id")
        if (
            local_max_before is not None
            and remote_max_id is not None
            and int(remote_max_id) < int(local_max_before)
        ):
            warnings.append(
                f"Remote max TradeMC ID ({remote_max_id}) is lower than local max ({local_max_before}); "
                "API token visibility may be restricted."
            )
    
    latest_local_trade_id = get_latest_local_trademc_trade_id() if incremental else None
    latest_local_date_updated = get_latest_local_trademc_date_updated() if incremental else None
    # Add overlap to avoid missing same-second updates at the incremental boundary.
    update_window_start = (
        _subtract_seconds_from_iso(latest_local_date_updated, seconds=120)
        if incremental and latest_local_date_updated
        else latest_local_date_updated
    )

    fetched_new: List[Dict[str, Any]] = []
    fetched_updates: List[Dict[str, Any]] = []
    fallback_recent: List[Dict[str, Any]] = []
    fetched_recent: List[Dict[str, Any]] = []
    remote_newer_detected = False

    if incremental:
        fetched_new_res = fetch_all_trademc_trades(
            progress_callback=progress_callback,
            min_trade_id=latest_local_trade_id,
        )
        if fetched_new_res is None:
            return {"success": False, "error": "Failed to fetch new trades from API", "count": 0}
        fetched_new = fetched_new_res

        if latest_local_date_updated:
            fetched_updates_res = fetch_all_trademc_trades(
                progress_callback=None,
                min_date_updated=update_window_start,
            )
            if fetched_updates_res is None:
                return {"success": False, "error": "Failed to fetch updated trades from API", "count": 0}
            fetched_updates = fetched_updates_res

            # Safety net: if filter-based incremental fetch returns nothing but the
            # remote latest date_updated is newer than local, pull recent updates directly.
            if not fetched_updates:
                latest_remote_res = fetch_trademc_trades(
                    limit=1,
                    offset=0,
                    fields=["id", "date_updated"],
                    sort="-date_updated,-id",
                )
                if latest_remote_res and isinstance(latest_remote_res.get("data"), list) and latest_remote_res["data"]:
                    remote_date = latest_remote_res["data"][0].get("date_updated")
                    remote_dt = _parse_utc_iso(remote_date)
                    local_dt = _parse_utc_iso(latest_local_date_updated)
                    remote_newer_detected = bool(
                        remote_dt is not None and local_dt is not None and remote_dt > local_dt
                    )

                if remote_newer_detected:
                    # Pull latest changed rows regardless of boundary filter.
                    fallback_res = fetch_trademc_trades(
                        limit=500,
                        offset=0,
                        sort="-date_updated,-id",
                    )
                    if fallback_res is None:
                        return {"success": False, "error": "Failed to fetch fallback updated trades", "count": 0}
                    fallback_recent = fallback_res.get("data", []) if isinstance(fallback_res, dict) else []

        # Always pull a small "recently updated" window to catch edits on existing
        # records even when boundary timestamps are awkward.
        recent_res = fetch_trademc_trades(
            limit=250,
            offset=0,
            sort="-date_updated,-id",
        )
        if recent_res is None:
            warnings.append("Could not fetch recent TradeMC updates window; continuing with incremental sets only.")
        elif isinstance(recent_res, dict):
            recent_data = recent_res.get("data")
            if isinstance(recent_data, list):
                fetched_recent = recent_data
    else:
        trades_res = fetch_all_trademc_trades(
            progress_callback=progress_callback,
            page_size=500,
            max_records=None,
        )
        if trades_res is None:
            print("[SYNC] Full fetch failed on first attempt; retrying full fetch once.")
            trades_res = fetch_all_trademc_trades(
                progress_callback=progress_callback,
                page_size=500,
                max_records=None,
            )
        if trades_res is None:
            probe = fetch_trademc_trades(
                limit=1,
                offset=0,
                fields=["id", "date_updated"],
                sort="-id",
                include_total_count=True,
                retry_attempts=2,
            )
            if isinstance(probe, dict):
                probe_rows = probe.get("data") if isinstance(probe.get("data"), list) else []
                probe_meta = probe.get("meta") if isinstance(probe.get("meta"), dict) else {}
                probe_msg = (
                    f"probe_ok total_count={probe_meta.get('total_count')} "
                    f"sample_rows={len(probe_rows)}"
                )
            else:
                probe_msg = "probe_failed"
            return {
                "success": False,
                "error": f"Failed to fetch trades from API ({probe_msg})",
                "count": 0,
            }
        fetched_new = trades_res

    trades = [*fetched_new, *fetched_updates, *fallback_recent, *fetched_recent]

    # Insert/update trades in database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    synced_at = datetime.now().isoformat()
    unique_by_id: Dict[int, Dict[str, Any]] = {}
    for trade in trades:
        trade_id = trade.get("id")
        if trade_id is not None:
            unique_by_id[int(trade_id)] = trade

    force_full_replace = bool(prune_missing and not incremental)
    existing_by_id: Dict[int, Dict[str, Any]] = {}
    ids_to_upsert: List[int] = []
    candidate_trade_ids: List[int] = list(unique_by_id.keys())
    if force_full_replace:
        ids_to_upsert = candidate_trade_ids
    else:
        if candidate_trade_ids:
            existing_by_id = _load_local_trade_compare_rows_by_ids(candidate_trade_ids)
        for trade_id, trade in unique_by_id.items():
            normalized_remote = _normalize_trade_compare_record(trade)
            if existing_by_id.get(trade_id) != normalized_remote:
                ids_to_upsert.append(trade_id)

    trade_rows: List[tuple] = []
    for trade_id in ids_to_upsert:
        trade = unique_by_id.get(trade_id, {})
        trade_rows.append((
            trade_id,
            trade.get('status'),
            trade.get('company_id'),
            trade.get('weight'),
            trade.get('notes'),
            trade.get('ref_number'),
            trade.get('trade_timestamp'),
            trade.get('zar_per_troy_ounce'),
            trade.get('zar_to_usd'),
            trade.get('requested_zar_per_gram'),
            trade.get('zar_per_troy_ounce_confirmed'),
            trade.get('zar_to_usd_confirmed'),
            trade.get('usd_per_troy_ounce_confirmed'),
            trade.get('date_created'),
            trade.get('date_updated'),
            1 if trade.get('evo_exported') else 0,
            synced_at
        ))

    if force_full_replace:
        expected_by_id: Dict[int, Dict[str, Any]] = {
            int(trade_id): _normalize_trade_compare_record(trade)
            for trade_id, trade in unique_by_id.items()
            if trade_id is not None
        }
        local_count_before = int(local_snapshot_before.get("count") or 0)
        cursor.execute("DELETE FROM trademc_trades")
        removed = int(
            cursor.rowcount
            if cursor.rowcount is not None and int(cursor.rowcount) >= 0
            else local_count_before
        )

        if trade_rows:
            cursor.executemany('''
                INSERT INTO trademc_trades (
                    id, status, company_id, weight, notes, ref_number,
                    trade_timestamp, zar_per_troy_ounce, zar_to_usd,
                    requested_zar_per_gram, zar_per_troy_ounce_confirmed,
                    zar_to_usd_confirmed, usd_per_troy_ounce_confirmed,
                    date_created, date_updated, evo_exported, last_synced
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', trade_rows)

        conn.commit()
        conn.close()
        verification = _verify_full_replace_sync(expected_by_id=expected_by_id, synced_at=synced_at)
        if not bool(verification.get("ok")):
            verification_error = (
                "Full replace verification failed: local cache does not match fetched remote snapshot."
            )
            warnings.append(verification_error)
            if int(_verify_retry) < 1:
                print("[SYNC] Full replace verification failed; retrying once.")
                retry_result = sync_trademc_trades(
                    progress_callback=progress_callback,
                    incremental=incremental,
                    prune_missing=prune_missing,
                    _verify_retry=int(_verify_retry) + 1,
                )
                retry_warnings = retry_result.get("warnings")
                if isinstance(retry_warnings, list):
                    retry_warnings.insert(0, verification_error)
                else:
                    retry_result["warnings"] = [verification_error]
                retry_result["verify_retry_used"] = True
                return retry_result

            local_snapshot_after = get_local_trademc_snapshot_stats()
            return {
                "success": False,
                "error": verification_error,
                "count": len(trade_rows),
                "inserted": len(trade_rows),
                "updated": 0,
                "removed": removed,
                "synced_at": synced_at,
                "mode": "full_replace",
                "start_after_id": latest_local_trade_id,
                "start_after_date_updated": latest_local_date_updated,
                "update_window_start": update_window_start,
                "fetched_new_count": len(fetched_new),
                "fetched_updated_count": len(fetched_updates),
                "fallback_recent_count": len(fallback_recent),
                "fetched_recent_count": len(fetched_recent),
                "remote_newer_detected": remote_newer_detected,
                "remote_snapshot": remote_snapshot,
                "local_snapshot_before": local_snapshot_before,
                "local_snapshot_after": local_snapshot_after,
                "warnings": warnings,
                "verification": verification,
                "verify_retry_used": bool(_verify_retry),
            }

        local_snapshot_after = get_local_trademc_snapshot_stats()

        return {
            "success": True,
            "count": len(trade_rows),
            "inserted": len(trade_rows),
            "updated": 0,
            "removed": removed,
            "synced_at": synced_at,
            "mode": "full_replace",
            "start_after_id": latest_local_trade_id,
            "start_after_date_updated": latest_local_date_updated,
            "update_window_start": update_window_start,
            "fetched_new_count": len(fetched_new),
            "fetched_updated_count": len(fetched_updates),
            "fallback_recent_count": len(fallback_recent),
            "fetched_recent_count": len(fetched_recent),
            "remote_newer_detected": remote_newer_detected,
            "remote_snapshot": remote_snapshot,
            "local_snapshot_before": local_snapshot_before,
            "local_snapshot_after": local_snapshot_after,
            "warnings": warnings,
            "verification": verification,
            "verify_retry_used": bool(_verify_retry),
            "message": "Full replace complete: local TradeMC trades table was rewritten from remote.",
        }

    remote_trade_ids = [int(trade_id) for trade_id in unique_by_id.keys()]
    inserted = 0
    updated = 0
    if trade_rows:
        existing_ids = set(existing_by_id.keys())
        inserted = sum(1 for trade_id in ids_to_upsert if trade_id not in existing_ids)
        updated = len(ids_to_upsert) - inserted
        cursor.executemany('''
            INSERT INTO trademc_trades (
                id, status, company_id, weight, notes, ref_number,
                trade_timestamp, zar_per_troy_ounce, zar_to_usd,
                requested_zar_per_gram, zar_per_troy_ounce_confirmed,
                zar_to_usd_confirmed, usd_per_troy_ounce_confirmed,
                date_created, date_updated, evo_exported, last_synced
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                company_id = excluded.company_id,
                weight = excluded.weight,
                notes = excluded.notes,
                ref_number = excluded.ref_number,
                trade_timestamp = excluded.trade_timestamp,
                zar_per_troy_ounce = excluded.zar_per_troy_ounce,
                zar_to_usd = excluded.zar_to_usd,
                requested_zar_per_gram = excluded.requested_zar_per_gram,
                zar_per_troy_ounce_confirmed = excluded.zar_per_troy_ounce_confirmed,
                zar_to_usd_confirmed = excluded.zar_to_usd_confirmed,
                usd_per_troy_ounce_confirmed = excluded.usd_per_troy_ounce_confirmed,
                date_created = excluded.date_created,
                date_updated = excluded.date_updated,
                evo_exported = excluded.evo_exported,
                last_synced = excluded.last_synced
        ''', trade_rows)

    removed = 0
    if prune_missing and not incremental:
        safe_to_prune = True
        local_count_before = int(local_snapshot_before.get("count") or 0)
        local_max_before = local_snapshot_before.get("max_id")
        if (
            remote_total_count is not None
            and local_count_before > 0
            and int(remote_total_count) < int(local_count_before * 0.8)
        ):
            safe_to_prune = False
            warnings.append(
                f"Prune skipped: remote total_count ({remote_total_count}) is much lower than local count ({local_count_before})."
            )
        if (
            local_max_before is not None
            and remote_max_id is not None
            and int(remote_max_id) < int(local_max_before)
        ):
            safe_to_prune = False
            warnings.append(
                f"Prune skipped: remote max ID ({remote_max_id}) is lower than local max ID ({local_max_before})."
            )

        if safe_to_prune and remote_trade_ids:
            cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _trademc_sync_remote_ids (id INTEGER PRIMARY KEY)")
            cursor.execute("DELETE FROM _trademc_sync_remote_ids")
            cursor.executemany(
                "INSERT OR IGNORE INTO _trademc_sync_remote_ids (id) VALUES (?)",
                [(trade_id,) for trade_id in remote_trade_ids],
            )
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM trademc_trades
                WHERE id NOT IN (SELECT id FROM _trademc_sync_remote_ids)
                """
            )
            removed = int(cursor.fetchone()[0] or 0)
            cursor.execute(
                """
                DELETE FROM trademc_trades
                WHERE id NOT IN (SELECT id FROM _trademc_sync_remote_ids)
                """
            )
        elif safe_to_prune and not remote_trade_ids:
            warnings.append("Prune skipped: remote TradeMC ID set is empty.")

    if not trade_rows and not (prune_missing and not incremental):
        conn.close()
        return {
            "success": True,
            "count": 0,
            "inserted": 0,
            "updated": 0,
            "removed": 0,
            "synced_at": synced_at,
            "mode": (
                "incremental"
                if incremental
                else ("full_replace" if prune_missing else "full")
            ),
            "start_after_id": latest_local_trade_id,
            "start_after_date_updated": latest_local_date_updated,
            "update_window_start": update_window_start,
            "fetched_new_count": len(fetched_new),
            "fetched_updated_count": len(fetched_updates),
            "fallback_recent_count": len(fallback_recent),
            "fetched_recent_count": len(fetched_recent),
            "remote_newer_detected": remote_newer_detected,
            "remote_snapshot": remote_snapshot,
            "local_snapshot_before": local_snapshot_before,
            "warnings": warnings,
            "message": (
                "No new or updated trades found."
                if incremental
                else "No trades were returned by the API."
            )
        }
    
    conn.commit()
    conn.close()
    local_snapshot_after = get_local_trademc_snapshot_stats()
    
    return {
        "success": True,
        "count": len(trade_rows),
        "inserted": inserted,
        "updated": updated,
        "removed": removed,
        "synced_at": synced_at,
        "mode": (
            "incremental"
            if incremental
            else ("full_replace" if prune_missing else "full")
        ),
        "start_after_id": latest_local_trade_id,
        "start_after_date_updated": latest_local_date_updated,
        "update_window_start": update_window_start,
        "fetched_new_count": len(fetched_new),
        "fetched_updated_count": len(fetched_updates),
        "fallback_recent_count": len(fallback_recent),
        "fetched_recent_count": len(fetched_recent),
        "remote_newer_detected": remote_newer_detected,
        "remote_snapshot": remote_snapshot,
        "local_snapshot_before": local_snapshot_before,
        "local_snapshot_after": local_snapshot_after,
        "warnings": warnings,
    }


def fetch_trademc_weight_transactions(
    limit: int = 100,
    offset: int = 0,
    fields: Optional[List[str]] = None,
    min_transaction_id: Optional[int] = None,
) -> Optional[Dict]:
    """
    Fetch weight transaction ledger entries from TradeMC Directus API.

    Args:
        limit: Number of items to retrieve per request
        offset: Offset for pagination
        fields: List of fields to return (optional)
        min_transaction_id: Only fetch rows with id greater than this value

    Returns:
        dict: API response containing weight transaction data, or None on error
    """
    url = f"{_trademc_base_url()}/items/weight_transaction_ledger"

    params = {
        "limit": limit,
        "offset": offset,
        "sort": "id",
    }

    if fields:
        params["fields"] = ",".join(fields)

    if min_transaction_id is not None:
        params["filter[id][_gt]"] = min_transaction_id

    try:
        response = requests.get(url, headers=get_api_headers(), params=params, timeout=30)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def get_latest_local_weight_transaction_id() -> Optional[int]:
    """Get the latest TradeMC weight transaction ID stored in the local database."""
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM trademc_weight_transactions")
    latest = cursor.fetchone()[0]
    conn.close()
    return int(latest) if latest is not None else None


def fetch_all_trademc_weight_transactions(
    progress_callback=None,
    page_size: int = 500,
    min_transaction_id: Optional[int] = None
) -> Optional[List[Dict]]:
    """
    Fetch weight transaction ledger entries from TradeMC API using pagination.

    Args:
        progress_callback: Optional callback function(fetched, total) for progress updates
        page_size: Number of rows to fetch per request
        min_transaction_id: Only fetch rows where ID is greater than this value

    Returns:
        list: Weight transaction records from the API, or None when an API error occurs
    """
    all_rows = []
    offset = 0
    limit = page_size
    total_count = None

    # First, get total count
    url = f"{_trademc_base_url()}/items/weight_transaction_ledger"
    try:
        response = requests.get(
            url,
            headers=get_api_headers(),
            params={
                "limit": 1,
                "meta": "total_count",
                **({"filter[id][_gt]": min_transaction_id} if min_transaction_id is not None else {}),
            },
            timeout=30
        )
        if response.status_code == 200:
            result = response.json()
            if "meta" in result:
                total_count = result["meta"].get("total_count", 0)
    except:
        pass

    while True:
        result = fetch_trademc_weight_transactions(
            limit=limit,
            offset=offset,
            min_transaction_id=min_transaction_id
        )

        if result is None:
            return None

        if result and "data" in result:
            rows = result["data"]
            if not rows:
                break
            all_rows.extend(rows)
            offset += len(rows)

            if progress_callback and total_count is not None:
                progress_callback(len(all_rows), total_count)

            if len(rows) < limit:
                break

            if offset > 200000:
                print("Warning: Stopping at 200000 records")
                break
        else:
            return None

    return all_rows


def sync_trademc_weight_transactions(
    progress_callback=None,
    page_size: int = 500,
    incremental: bool = True
) -> Dict[str, Any]:
    """
    Sync TradeMC weight transaction ledger entries from API to local database.

    Args:
        progress_callback: Optional callback function for progress updates
        page_size: Number of rows to fetch per request
        incremental: When True, only fetch rows with ID > latest local ID

    Returns:
        dict: Sync result with counts
    """
    initialize_trademc_table()

    latest_local_tx_id = get_latest_local_weight_transaction_id() if incremental else None
    rows = fetch_all_trademc_weight_transactions(
        progress_callback=progress_callback,
        page_size=page_size,
        min_transaction_id=latest_local_tx_id
    )

    if rows is None:
        return {"success": False, "error": "Failed to fetch weight transactions from API", "count": 0}

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    synced_at = datetime.now().isoformat()
    unique_by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        row_id = row.get("id")
        if row_id is not None:
            unique_by_id[int(row_id)] = row

    row_values: List[tuple] = []
    for row_id, row in unique_by_id.items():
        row_values.append((
            row.get('user_created'),
            row.get('date_created'),
            row.get('user_updated'),
            row.get('date_updated'),
            row.get('company'),
            row.get('pc_code'),
            row.get('notes'),
            row.get('type'),
            row.get('weight'),
            row.get('rolling_balance'),
            row.get('transaction_timestamp'),
            row.get('gold_percentage'),
            row.get('old_id'),
            row.get('reversal_id'),
            row.get('trade'),
            synced_at,
            row_id,
        ))

    if not row_values:
        conn.close()
        return {
            "success": True,
            "count": 0,
            "inserted": 0,
            "updated": 0,
            "synced_at": synced_at,
            "mode": "incremental" if incremental else "full",
            "start_after_id": latest_local_tx_id,
            "message": "No new weight transactions found.",
        }

    row_ids = [row[-1] for row in row_values]
    existing_ids = set()
    chunk_size = 900
    for i in range(0, len(row_ids), chunk_size):
        chunk = row_ids[i:i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        cursor.execute(f"SELECT id FROM trademc_weight_transactions WHERE id IN ({placeholders})", chunk)
        existing_ids.update(item[0] for item in cursor.fetchall())

    inserted = sum(1 for row_id in row_ids if row_id not in existing_ids)
    updated = len(row_values) - inserted

    cursor.executemany('''
        INSERT INTO trademc_weight_transactions (
            user_created, date_created, user_updated, date_updated,
            company_id, pc_code, notes, type, weight, rolling_balance,
            transaction_timestamp, gold_percentage, old_id, reversal_id,
            trade_id, last_synced, id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            user_created = excluded.user_created,
            date_created = excluded.date_created,
            user_updated = excluded.user_updated,
            date_updated = excluded.date_updated,
            company_id = excluded.company_id,
            pc_code = excluded.pc_code,
            notes = excluded.notes,
            type = excluded.type,
            weight = excluded.weight,
            rolling_balance = excluded.rolling_balance,
            transaction_timestamp = excluded.transaction_timestamp,
            gold_percentage = excluded.gold_percentage,
            old_id = excluded.old_id,
            reversal_id = excluded.reversal_id,
            trade_id = excluded.trade_id,
            last_synced = excluded.last_synced
    ''', row_values)

    conn.commit()
    conn.close()

    return {
        "success": True,
        "count": len(row_values),
        "inserted": inserted,
        "updated": updated,
        "synced_at": synced_at,
        "mode": "incremental" if incremental else "full",
        "start_after_id": latest_local_tx_id,
    }


def load_trademc_trades(
    status: Optional[str] = None,
    notes_filter: Optional[str] = None,
    ref_filter: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    company_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Load TradeMC trades from local database with optional filters.
    
    Args:
        status: Filter by status (e.g., 'confirmed')
        notes_filter: Filter by notes containing this string
        ref_filter: Filter by trade reference containing this string
        start_date: Filter trades on or after this date
        end_date: Filter trades on or before this date
        company_id: Filter by company ID
    
    Returns:
        DataFrame: Filtered trade data
    """
    # Ensure table exists
    initialize_trademc_table()
    
    conn = sqlite3.connect(DB_PATH)
    
    query = "SELECT * FROM trademc_trades WHERE 1=1"
    params = []
    
    if status:
        query += " AND status = ?"
        params.append(status)
    
    if notes_filter:
        query += " AND notes LIKE ?"
        params.append(f"%{notes_filter}%")

    if ref_filter:
        query += " AND ref_number LIKE ?"
        params.append(f"%{ref_filter}%")
    
    if start_date:
        query += " AND trade_timestamp >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND trade_timestamp <= ?"
        params.append(end_date)
    
    if company_id:
        query += " AND company_id = ?"
        params.append(company_id)
    
    query += " ORDER BY trade_timestamp DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    # Convert timestamp columns
    for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df


def get_trademc_trade_by_id(trade_id: int) -> Optional[Dict]:
    """
    Get a single TradeMC trade by ID.
    
    Args:
        trade_id: The trade ID to fetch
    
    Returns:
        dict: Trade data or None if not found
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM trademc_trades WHERE id = ?', (trade_id,))
    row = cursor.fetchone()
    
    if row:
        columns = [description[0] for description in cursor.description]
        trade = dict(zip(columns, row))
        conn.close()
        return trade
    
    conn.close()
    return None


def get_trademc_stats() -> Dict[str, Any]:
    """
    Get statistics about TradeMC trades in the database.
    
    Returns:
        dict: Statistics including count, date range, total weight, etc.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trademc_trades'")
    if not cursor.fetchone():
        conn.close()
        return {"count": 0, "synced": False}
    
    cursor.execute('SELECT COUNT(*) FROM trademc_trades')
    count = cursor.fetchone()[0]
    
    if count == 0:
        conn.close()
        return {"count": 0, "synced": False}
    
    cursor.execute('SELECT MIN(trade_timestamp), MAX(trade_timestamp) FROM trademc_trades')
    min_date, max_date = cursor.fetchone()
    
    cursor.execute('SELECT SUM(weight) FROM trademc_trades')
    total_weight = cursor.fetchone()[0] or 0
    
    cursor.execute('SELECT MAX(last_synced) FROM trademc_trades')
    last_synced = cursor.fetchone()[0]
    
    cursor.execute('SELECT status, COUNT(*) FROM trademc_trades GROUP BY status')
    status_counts = dict(cursor.fetchall())
    
    conn.close()
    
    return {
        "count": count,
        "synced": True,
        "min_date": min_date,
        "max_date": max_date,
        "total_weight": total_weight,
        "last_synced": last_synced,
        "status_counts": status_counts
    }


def get_unique_companies() -> List[int]:
    """Get list of unique company IDs in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT DISTINCT company_id FROM trademc_trades WHERE company_id IS NOT NULL ORDER BY company_id')
    companies = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return companies


def get_unique_statuses() -> List[str]:
    """Get list of unique statuses in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT DISTINCT status FROM trademc_trades WHERE status IS NOT NULL ORDER BY status')
    statuses = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return statuses


def get_unique_weight_types() -> List[str]:
    """Get list of unique weight transaction types in the database."""
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT DISTINCT type FROM trademc_weight_transactions WHERE type IS NOT NULL ORDER BY type')
    types = [row[0] for row in cursor.fetchall()]

    conn.close()
    return types


def get_trademc_trades_for_trade_number(trade_number: str) -> pd.DataFrame:
    """
    Get TradeMC trades that match a specific trade number (from notes field).
    
    Args:
        trade_number: The trade number to search for (e.g., "P1019")
    
    Returns:
        DataFrame: Matching trades with their details including company name
    """
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    
    # Search for trades where notes contains the trade number, join with companies
    query = """
        SELECT t.*, c.company_name 
        FROM trademc_trades t
        LEFT JOIN trademc_companies c ON t.company_id = c.id
        WHERE t.notes LIKE ? 
        ORDER BY t.trade_timestamp DESC
    """
    
    df = pd.read_sql_query(query, conn, params=[f"%{trade_number}%"])
    conn.close()
    
    # Convert timestamp columns
    for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df


def fetch_all_companies() -> List[Dict]:
    """
    Fetch all companies from TradeMC API.
    
    Returns:
        list: All company records from the API
    """
    url = f"{_trademc_base_url()}/items/company"
    
    try:
        response = requests.get(url, headers=get_api_headers(), timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get("data", [])
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []


def sync_trademc_companies() -> Dict[str, Any]:
    """
    Sync all companies from TradeMC API to local database.
    
    Returns:
        dict: Sync result with counts
    """
    # Ensure table exists
    initialize_trademc_table()
    
    # Fetch all companies from API
    companies = fetch_all_companies()
    
    if not companies:
        return {"success": False, "error": "No companies fetched from API", "count": 0}
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    synced_at = datetime.now().isoformat()
    inserted = 0
    updated = 0
    
    for company in companies:
        # Check if company already exists
        cursor.execute('SELECT id FROM trademc_companies WHERE id = ?', (company['id'],))
        exists = cursor.fetchone()
        
        if exists:
            # Update existing record
            cursor.execute('''
                UPDATE trademc_companies SET
                    status = ?,
                    company_name = ?,
                    registration_number = ?,
                    contact_number = ?,
                    email_address = ?,
                    sharepoint_identifier = ?,
                    trade_limit = ?,
                    blocked = ?,
                    vat_number = ?,
                    EVO_customer_code = ?,
                    refining_rate = ?,
                    date_created = ?,
                    date_updated = ?,
                    last_synced = ?
                WHERE id = ?
            ''', (
                company.get('status'),
                company.get('company_name'),
                company.get('registration_number'),
                company.get('contact_number'),
                company.get('email_address'),
                company.get('sharepoint_identifier'),
                company.get('trade_limit'),
                1 if company.get('blocked') else 0,
                str(company.get('vat_number', '')),
                company.get('EVO_customer_code'),
                company.get('refining_rate'),
                company.get('date_created'),
                company.get('date_updated'),
                synced_at,
                company['id']
            ))
            updated += 1
        else:
            # Insert new record
            cursor.execute('''
                INSERT INTO trademc_companies (
                    id, status, company_name, registration_number, contact_number,
                    email_address, sharepoint_identifier, trade_limit, blocked,
                    vat_number, EVO_customer_code, refining_rate,
                    date_created, date_updated, last_synced
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                company['id'],
                company.get('status'),
                company.get('company_name'),
                company.get('registration_number'),
                company.get('contact_number'),
                company.get('email_address'),
                company.get('sharepoint_identifier'),
                company.get('trade_limit'),
                1 if company.get('blocked') else 0,
                str(company.get('vat_number', '')),
                company.get('EVO_customer_code'),
                company.get('refining_rate'),
                company.get('date_created'),
                company.get('date_updated'),
                synced_at
            ))
            inserted += 1
    
    conn.commit()
    conn.close()
    
    return {
        "success": True,
        "count": len(companies),
        "inserted": inserted,
        "updated": updated,
        "synced_at": synced_at
    }


def get_company_name(company_id: int) -> Optional[str]:
    """
    Get company name by ID.
    
    Args:
        company_id: The company ID
    
    Returns:
        str: Company name or None if not found
    """
    if not company_id:
        return None

    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT company_name FROM trademc_companies WHERE id = ?', (company_id,))
    row = cursor.fetchone()
    
    conn.close()
    return row[0] if row else None


def load_trademc_trades_with_companies(
    status: Optional[str] = None,
    notes_filter: Optional[str] = None,
    ref_filter: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    company_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Load TradeMC trades with company names joined from local database.
    
    Args:
        status: Filter by status
        notes_filter: Filter by notes containing this string
        ref_filter: Filter by trade reference containing this string
        start_date: Filter trades on or after this date
        end_date: Filter trades on or before this date
        company_id: Filter by company ID
    
    Returns:
        DataFrame: Filtered trade data with company names
    """
    # Ensure table exists
    initialize_trademc_table()
    
    conn = sqlite3.connect(DB_PATH)
    
    query = """
        SELECT t.*, c.company_name, c.refining_rate as company_refining_rate
        FROM trademc_trades t
        LEFT JOIN trademc_companies c ON t.company_id = c.id
        WHERE 1=1
    """
    params = []
    
    if status:
        query += " AND t.status = ?"
        params.append(status)
    
    if notes_filter:
        query += " AND t.notes LIKE ?"
        params.append(f"%{notes_filter}%")

    if ref_filter:
        query += " AND t.ref_number LIKE ?"
        params.append(f"%{ref_filter}%")
    
    if start_date:
        query += " AND t.trade_timestamp >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND t.trade_timestamp <= ?"
        params.append(end_date)
    
    if company_id:
        query += " AND t.company_id = ?"
        params.append(company_id)
    
    query += " ORDER BY t.trade_timestamp DESC"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    # Convert timestamp columns
    for col in ['trade_timestamp', 'date_created', 'date_updated', 'last_synced']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df


def load_trademc_weight_transactions_with_companies(
    company_id: Optional[int] = None,
    tx_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Load TradeMC weight transaction ledger with company names joined from local database.

    Args:
        company_id: Filter by company ID
        tx_type: Filter by transaction type
        start_date: Filter transactions on or after this date
        end_date: Filter transactions on or before this date

    Returns:
        DataFrame: Filtered weight transaction data with company names
    """
    initialize_trademc_table()

    conn = sqlite3.connect(DB_PATH)

    query = """
        SELECT w.*, c.company_name
        FROM trademc_weight_transactions w
        LEFT JOIN trademc_companies c ON w.company_id = c.id
        WHERE 1=1
    """
    params = []

    if company_id:
        query += " AND w.company_id = ?"
        params.append(company_id)

    if tx_type:
        query += " AND w.type = ?"
        params.append(tx_type)

    if start_date:
        query += " AND w.transaction_timestamp >= ?"
        params.append(start_date)

    if end_date:
        query += " AND w.transaction_timestamp <= ?"
        params.append(end_date)

    query += " ORDER BY w.transaction_timestamp DESC, w.id DESC"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    for col in ['transaction_timestamp', 'date_created', 'date_updated', 'last_synced']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    return df


def get_all_companies() -> pd.DataFrame:
    """
    Get all companies from the local database.
    
    Returns:
        DataFrame: All company records
    """
    initialize_trademc_table()
    conn = sqlite3.connect(DB_PATH)
    
    df = pd.read_sql_query(
        "SELECT * FROM trademc_companies ORDER BY company_name",
        conn
    )
    conn.close()
    
    return df
