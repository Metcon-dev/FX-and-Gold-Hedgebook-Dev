"""
SharePoint Service — Supplier Payment Recon Integration

Connects to SharePoint Online, lists/downloads Excel recon files,
and parses them into structured DataFrames for display in the platform.

Auth: username / password via Office365-REST-Python-Client.
"""
import io
import os
import re
import time
import threading
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONFIGURATION (reads from env vars set by server.py's _load_env_file)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _cfg(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CACHE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {
    "suppliers": None,          # list of {name, filename}
    "data": {},                 # {supplier_name: [{row}, ...]}
    "last_sync": None,          # datetime
    "ttl_seconds": 300,         # 5 min cache TTL
}


def _cache_is_valid() -> bool:
    if _cache["last_sync"] is None:
        return False
    age = (datetime.now() - _cache["last_sync"]).total_seconds()
    return age < _cache["ttl_seconds"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SHAREPOINT CONNECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_msal_app = None
_msal_lock = threading.Lock()


def _get_msal_app():
    """Get or create a persistent MSAL PublicClientApplication with token cache."""
    global _msal_app
    with _msal_lock:
        if _msal_app is not None:
            return _msal_app
        import msal
        import json

        client_id = _cfg("SHAREPOINT_CLIENT_ID", "d3590ed6-52b3-4102-aeff-aad2292ab01c")
        tenant = _cfg("SHAREPOINT_TENANT", "metalconcentrators.onmicrosoft.com")
        authority = f"https://login.microsoftonline.com/{tenant}"

        # Persistent token cache
        cache = msal.SerializableTokenCache()
        cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sharepoint_token_cache.json")
        if os.path.isfile(cache_file):
            try:
                with open(cache_file, "r") as f:
                    cache.deserialize(f.read())
            except Exception:
                pass

        app = msal.PublicClientApplication(
            client_id,
            authority=authority,
            token_cache=cache,
        )

        _msal_app = app
        return app


def _save_msal_cache():
    """Persist the MSAL token cache to disk."""
    global _msal_app
    if _msal_app is None:
        return
    cache = _msal_app.token_cache
    if cache.has_state_changed:
        cache_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sharepoint_token_cache.json")
        with open(cache_file, "w") as f:
            f.write(cache.serialize())


def _get_client():
    """Create an authenticated SharePoint ClientContext.

    Uses MSAL device code flow (works with MFA, no app registration needed).
    First call: prints a URL and code to the console for you to authenticate.
    Subsequent calls: uses cached token silently.
    """
    import msal
    from office365.sharepoint.client_context import ClientContext

    site_url = _cfg("SHAREPOINT_URL", "https://metalconcentrators.sharepoint.com")
    scopes = [f"{site_url}/.default"]

    app = _get_msal_app()

    # Try silent token acquisition first
    accounts = app.get_accounts()
    result = None
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])

    # If no cached token, use device code flow
    if not result or "access_token" not in result:
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise ValueError(f"Device flow initiation failed: {flow.get('error_description', 'Unknown error')}")

        # Print instructions to console
        print("\n" + "=" * 60)
        print("  SHAREPOINT LOGIN REQUIRED")
        print("=" * 60)
        print(f"  1. Go to: {flow['verification_uri']}")
        print(f"  2. Enter code: {flow['user_code']}")
        print("  3. Sign in with your Microsoft account")
        print("=" * 60 + "\n")
        logger.info("SharePoint: device code auth - go to %s and enter code %s",
                     flow['verification_uri'], flow['user_code'])

        result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        error_desc = result.get("error_description", result.get("error", "Unknown error"))
        raise ValueError(f"SharePoint auth failed: {error_desc}")

    _save_msal_cache()

    access_token = result["access_token"]
    ctx = ClientContext(site_url).with_access_token(lambda: access_token)
    return ctx


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FILE LISTING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _extract_supplier_name(filename: str) -> str:
    """Extract supplier code from filename.

    Examples:
        'FOR - Payment Recon.xlsx'  -> 'FOR'
        'GCG_Recon_2025.xlsx'       -> 'GCG'
        'FOR.xlsx'                  -> 'FOR'
    """
    name = os.path.splitext(filename)[0]
    # Try common patterns: "CODE - rest" or "CODE_rest" or just "CODE"
    m = re.match(r'^([A-Z0-9]{2,10})\s*[-_]', name, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # Fallback: use entire name (cleaned)
    return re.sub(r'[^A-Za-z0-9]', '_', name).strip('_').upper()


def list_recon_files(force_refresh: bool = False) -> List[Dict[str, str]]:
    """List all .xlsx files in the SUPPLIER PAYMENT RECONS folder.

    Returns: [{"name": "FOR", "filename": "FOR - Recon.xlsx"}, ...]
    """
    with _cache_lock:
        if not force_refresh and _cache["suppliers"] is not None and _cache_is_valid():
            return _cache["suppliers"]

    folder_path = _cfg("SHAREPOINT_RECON_FOLDER", "/Bullion/Trading Models/SUPPLIER PAYMENT RECONS")
    # Ensure the folder path uses forward slashes and no trailing slash
    folder_path = folder_path.replace("\\", "/").rstrip("/")

    ctx = _get_client()
    folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()

    suppliers = []
    for f in files:
        fname = f.properties.get("Name", "")
        if fname.lower().endswith(".xlsx") and not fname.startswith("~$"):
            suppliers.append({
                "name": _extract_supplier_name(fname),
                "filename": fname,
            })

    suppliers.sort(key=lambda x: x["name"])

    with _cache_lock:
        _cache["suppliers"] = suppliers

    logger.info("SharePoint: found %d supplier recon files", len(suppliers))
    return suppliers


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FILE DOWNLOAD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _download_file(filename: str) -> io.BytesIO:
    """Download a single Excel file from SharePoint into memory."""
    folder_path = _cfg("SHAREPOINT_RECON_FOLDER", "/Bullion/Trading Models/SUPPLIER PAYMENT RECONS")
    folder_path = folder_path.replace("\\", "/").rstrip("/")
    file_url = f"{folder_path}/{filename}"

    ctx = _get_client()
    buf = io.BytesIO()
    file_obj = ctx.web.get_file_by_server_relative_url(file_url)
    file_obj.download(buf).execute_query()
    buf.seek(0)

    logger.info("SharePoint: downloaded '%s' (%d bytes)", filename, buf.getbuffer().nbytes)
    return buf


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EXCEL PARSING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Expected columns (normalised). We'll map from raw headers.
EXPECTED_COLUMNS = [
    "trade_date",
    "transaction_type",
    "trade_delivery_ref",
    "au_price_r_usd",
    "grams_hedges_dt",
    "spot_r_gram",
    "grams_delivered_ct",
    "grams_balance_due",
    "advance_option",
    "advance_payment_zar",
    "advance_grv",
    "debit_credit_note",
    "balance_payment_zar",
    "inv_nr",
    "au_pct",
    "balance_due_zar",
    "trade_value_zar",
    "refining_fee",
    "buy_r_gram_exc_vat",
    "buy_r_gram_drc_vat",
    "advance_95pct",
    "check_2_1pct",
]

# Map raw header substrings to normalised column names
_HEADER_MAP = {
    "trade date":           "trade_date",
    "transaction type":     "transaction_type",
    "trade/delivery":       "trade_delivery_ref",
    "delivery reference":   "trade_delivery_ref",
    "au price":             "au_price_r_usd",
    "r/$":                  "au_price_r_usd",
    "grams hedges":         "grams_hedges_dt",
    "hedges (dt)":          "grams_hedges_dt",
    "spot r/gram":          "spot_r_gram",
    "grams delivered":      "grams_delivered_ct",
    "delivered (ct)":       "grams_delivered_ct",
    "grams balance due":    "grams_balance_due",
    "balance due":          "grams_balance_due",
    "available":            "grams_balance_due",
    "advance option":       "advance_option",
    "advance payment":      "advance_payment_zar",
    "advance grv":          "advance_grv",
    "debit/credit":         "debit_credit_note",
    "credit note":          "debit_credit_note",
    "balance payment":      "balance_payment_zar",
    "inv nr":               "inv_nr",
    "invoice":              "inv_nr",
    "au%":                  "au_pct",
    "balance due (zar)":    "balance_due_zar",
    "trade value":          "trade_value_zar",
    "refining fee":         "refining_fee",
    "buy r/gram (exc":      "buy_r_gram_exc_vat",
    "exc vat":              "buy_r_gram_exc_vat",
    "buy r/gram (drc":      "buy_r_gram_drc_vat",
    "drc vat":              "buy_r_gram_drc_vat",
    "95% advance":          "advance_95pct",
    "check 2.1":            "check_2_1pct",
    "check":                "check_2_1pct",
}


def _normalise_header(raw: str) -> str:
    """Map a raw Excel header to a normalised column name."""
    cleaned = str(raw).strip().lower()
    # Try exact substring matches, longest first
    for pattern, col in sorted(_HEADER_MAP.items(), key=lambda x: -len(x[0])):
        if pattern in cleaned:
            return col
    # Fallback: slugify
    slug = re.sub(r'[^a-z0-9]+', '_', cleaned).strip('_')
    return slug or "unknown"


def _safe_float(val: Any) -> Optional[float]:
    """Convert to float, handling currency strings like 'R 1,234.56'."""
    if val is None or (isinstance(val, float) and (pd.isna(val) or pd.isinf(val))):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s == "--" or s == "-" or s.lower() == "nan":
        return None
    # Remove currency prefixes and thousands separators
    s = re.sub(r'^[R$\s]+', '', s)
    s = s.replace(',', '').replace(' ', '')
    # Handle parentheses for negatives: (123) -> -123
    m = re.match(r'^\(([0-9.]+)\)$', s)
    if m:
        s = f"-{m.group(1)}"
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _safe_date(val: Any) -> Optional[str]:
    """Convert to ISO date string."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if not s or s == "--" or s.lower() == "nan" or s.lower() == "nat":
        return None
    # Try common formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s  # Return as-is if no format matches


def _safe_str(val: Any) -> Optional[str]:
    """Convert to string or None."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


# Columns that should be numeric
_NUMERIC_COLS = {
    "au_price_r_usd", "grams_hedges_dt", "spot_r_gram",
    "grams_delivered_ct", "grams_balance_due", "advance_option",
    "advance_payment_zar", "debit_credit_note", "balance_payment_zar",
    "au_pct", "balance_due_zar", "trade_value_zar", "refining_fee",
    "buy_r_gram_exc_vat", "buy_r_gram_drc_vat", "advance_95pct",
    "check_2_1pct",
}

_DATE_COLS = {"trade_date"}
_STR_COLS = {"transaction_type", "trade_delivery_ref", "advance_grv", "inv_nr"}


def parse_recon_excel(file_bytes: io.BytesIO, supplier_name: str) -> List[Dict[str, Any]]:
    """Parse a supplier recon Excel file into a list of row dicts.

    Handles multi-sheet workbooks (uses first sheet), auto-detects
    the header row, normalises column names, and cleans data types.
    """
    try:
        # Read raw — try to auto-detect header
        df_raw = pd.read_excel(file_bytes, sheet_name=0, header=None, dtype=str)
    except Exception as e:
        logger.warning("Failed to read Excel for %s: %s", supplier_name, e)
        return []

    if df_raw.empty:
        return []

    # Find header row — look for a row containing "Trade Date" or similar
    header_row_idx = 0
    for idx, row in df_raw.iterrows():
        row_text = " ".join(str(v).lower() for v in row.values if pd.notna(v))
        if "trade date" in row_text or "transaction type" in row_text:
            header_row_idx = idx
            break

    # Re-read with correct header
    file_bytes.seek(0)
    df = pd.read_excel(
        file_bytes,
        sheet_name=0,
        header=header_row_idx,
        dtype=str,
    )

    # Normalise column names
    col_mapping = {}
    used_names = set()
    for raw_col in df.columns:
        norm = _normalise_header(raw_col)
        # Handle duplicates
        if norm in used_names:
            suffix = 2
            while f"{norm}_{suffix}" in used_names:
                suffix += 1
            norm = f"{norm}_{suffix}"
        used_names.add(norm)
        col_mapping[raw_col] = norm
    df.rename(columns=col_mapping, inplace=True)

    # Convert to list of dicts with proper types
    rows = []
    for _, row in df.iterrows():
        record: Dict[str, Any] = {"supplier": supplier_name}

        # Check if entire row is empty
        non_null = sum(1 for v in row.values if pd.notna(v) and str(v).strip())
        if non_null < 2:
            continue

        for col in df.columns:
            val = row.get(col)
            if col in _DATE_COLS:
                record[col] = _safe_date(val)
            elif col in _NUMERIC_COLS:
                record[col] = _safe_float(val)
            elif col in _STR_COLS:
                record[col] = _safe_str(val)
            else:
                # Try numeric first, fallback to string
                nv = _safe_float(val)
                if nv is not None:
                    record[col] = nv
                else:
                    record[col] = _safe_str(val)

        rows.append(record)

    logger.info("Parsed %d rows for supplier %s", len(rows), supplier_name)
    return rows


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SYNC / FETCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def sync_all_recons(force: bool = False) -> Dict[str, Any]:
    """Download and parse all supplier recon files from SharePoint.

    Returns: {"suppliers": [...], "data": {name: [rows]}, "synced_at": "..."}
    """
    with _cache_lock:
        if not force and _cache_is_valid() and _cache["data"]:
            return {
                "suppliers": _cache["suppliers"],
                "data": _cache["data"],
                "synced_at": _cache["last_sync"].isoformat() if _cache["last_sync"] else None,
                "cached": True,
            }

    suppliers = list_recon_files(force_refresh=force)
    data: Dict[str, List[Dict[str, Any]]] = {}

    for sup in suppliers:
        try:
            buf = _download_file(sup["filename"])
            rows = parse_recon_excel(buf, sup["name"])
            data[sup["name"]] = rows
        except Exception as e:
            logger.error("Failed to sync recon for %s: %s", sup["name"], e)
            data[sup["name"]] = []

    with _cache_lock:
        _cache["suppliers"] = suppliers
        _cache["data"] = data
        _cache["last_sync"] = datetime.now()

    return {
        "suppliers": suppliers,
        "data": data,
        "synced_at": _cache["last_sync"].isoformat(),
        "cached": False,
    }


def get_supplier_recon(supplier_name: str, force: bool = False) -> Dict[str, Any]:
    """Get recon data for a single supplier.

    Downloads only the requested file if not cached.
    """
    name_upper = supplier_name.upper().strip()

    # Check cache first
    with _cache_lock:
        if not force and _cache_is_valid() and name_upper in _cache.get("data", {}):
            return {
                "supplier": name_upper,
                "rows": _cache["data"][name_upper],
                "row_count": len(_cache["data"][name_upper]),
                "synced_at": _cache["last_sync"].isoformat() if _cache["last_sync"] else None,
                "cached": True,
            }

    # Find the file for this supplier
    suppliers = list_recon_files()
    match = None
    for s in suppliers:
        if s["name"] == name_upper:
            match = s
            break

    if match is None:
        return {
            "supplier": name_upper,
            "rows": [],
            "row_count": 0,
            "error": f"No recon file found for supplier '{name_upper}'",
        }

    try:
        buf = _download_file(match["filename"])
        rows = parse_recon_excel(buf, name_upper)
    except Exception as e:
        logger.error("Failed to download recon for %s: %s", name_upper, e)
        return {
            "supplier": name_upper,
            "rows": [],
            "row_count": 0,
            "error": str(e),
        }

    # Update cache for this supplier
    with _cache_lock:
        if _cache["data"] is None:
            _cache["data"] = {}
        _cache["data"][name_upper] = rows
        _cache["last_sync"] = datetime.now()

    return {
        "supplier": name_upper,
        "rows": rows,
        "row_count": len(rows),
        "synced_at": datetime.now().isoformat(),
        "cached": False,
    }


def get_cached_suppliers() -> List[Dict[str, str]]:
    """Return cached supplier list without hitting SharePoint."""
    with _cache_lock:
        return _cache["suppliers"] or []
