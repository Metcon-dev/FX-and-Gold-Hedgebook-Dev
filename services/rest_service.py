"""REST API client for pulling trades (StoneX example)."""
from typing import Any, Dict, List, Optional
import os
import json
import re
import requests


def login_get_token(
    host: str,
    subscription_key: str,
    username: str,
    password: str,
) -> Dict:
    """
    POST to the StoneX login endpoint to obtain an access token.

    Returns dict with ok/bool and access_token on success.
    """
    if not subscription_key:
        return {"ok": False, "error": "Missing subscription key"}
    if not username or not password:
        return {"ok": False, "error": "Missing username/password"}

    url = f"https://{host}/authentication/login"
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    payload = {"username": username, "password": password}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        data = resp.json() if resp.content else {}
        if resp.ok and "accessToken" in data:
            return {
                "ok": True,
                "status": resp.status_code,
                "access_token": data.get("accessToken"),
                "refresh_token": data.get("refreshToken", ""),
                "raw": data,
            }
        return {
            "ok": False,
            "status": resp.status_code,
            "error": data.get("message", resp.reason),
            "raw": data,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_trades_via_rest(
    host: str,
    path: str,
    access_token: Optional[str] = None,
    subscription_key: Optional[str] = None,
    source_system: str = "GMI,Murex,XTP",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account_number: str = "",
    group_account: str = "",
    fields: str = "",
    output: str = "csv",
    page_size: str = "",
    page_number: str = "",
) -> Dict:
    """
    Call the REST trades endpoint and return response metadata + body.

    Per StoneX spec:
    - Authorization: Bearer <accessToken> from /authentication/login
    - Header: Ocp-Apim-Subscription-Key: <subscription key>
    """
    token = access_token or os.getenv("STONEX_ACCESS_TOKEN", "")
    sub_key = subscription_key or os.getenv("STONEX_SUBSCRIPTION_KEY", "")
    if not token:
        return {"ok": False, "error": "Missing access token"}
    if not sub_key:
        return {"ok": False, "error": "Missing subscription key"}

    url = f"https://{host}{path}"

    params = {
        "sourceSystem": source_system,
        "startDate": start_date or "",
        "endDate": end_date or "",
        "accountNumber": account_number or "",
        "groupAccount": group_account or "",
        "fields": fields or "",
        "output": output or "csv",
        "pageSize": page_size or "",
        "pageNumber": page_number or "",
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept": "application/json, text/csv",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        content_type = resp.headers.get("Content-Type", "")
        body_text = resp.text

        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type,
            "body": body_text,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_historical_trades(
    host: str,
    access_token: str,
    subscription_key: str,
    start_date: str,
    end_date: str,
    source_system: str = "GMI,Murex,XTP",
    account_number: str = "",
    group_account: str = "",
    fields: str = "",
    output: str = "json",
    page_size: str = "",
    page_number: str = "",
    path: str = "/global-trades/history",
) -> Dict:
    """
    GET /global-trades/history
    Retrieves historical trades by account number from StoneX API.
    
    Required Parameters:
        host: API host (e.g., "api.stonex.com")
        access_token: Bearer token from authentication
        subscription_key: Ocp-Apim-Subscription-Key
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
    
    Optional Parameters:
        source_system: GMI, Murex, XTP (default: all)
        account_number: Specific account number
        group_account: Group account number
        fields: Comma-separated list of fields
        output: Response format - csv or json (default: json)
        page_size: Results per page
        page_number: Page number
    
    Returns:
        Dict with ok, status, body, and content_type
    """
    if not access_token:
        return {"ok": False, "error": "Missing access token"}
    if not subscription_key:
        return {"ok": False, "error": "Missing subscription key"}
    if not start_date or not end_date:
        return {"ok": False, "error": "Missing required startDate or endDate"}

    url = f"https://{host}{path}"
    
    # Build query parameters - only include non-empty values
    params = {
        "sourceSystem": source_system,
        "startDate": start_date,
        "endDate": end_date,
    }
    if account_number:
        params["accountNumber"] = account_number
    if group_account:
        params["groupAccount"] = group_account
    if fields:
        params["fields"] = fields
    if output:
        params["output"] = output
    if page_size:
        params["pageSize"] = page_size
    if page_number:
        params["pageNumber"] = page_number

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept": "application/json, text/csv",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        content_type = resp.headers.get("Content-Type", "")
        body_text = resp.text

        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type,
            "body": body_text,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_intraday_trades(
    host: str,
    access_token: str,
    subscription_key: str,
    group_account: str = "",
    account_number: str = "",
    fields: str = "",
    source_system: str = "GMI,Murex,XTP",
    output: str = "csv",
    path: str = "/global-trades/intraday",
) -> Dict:
    """
    GET /global-trades/intraday
    Retrieves intraday trades by account number from StoneX API.
    
    Args:
        host: API host (e.g., "api.stonex.com")
        access_token: Bearer token from authentication
        subscription_key: Ocp-Apim-Subscription-Key
        group_account: Group account number (optional)
        account_number: Account number (optional)
        fields: Comma-separated list of fields (optional)
        source_system: Source systems - GMI, Murex, XTP (default: all)
        output: Response format - csv or json (default: csv)
    
    Returns:
        Dict with ok, status, body, and content_type
    """
    if not access_token:
        return {"ok": False, "error": "Missing access token"}
    if not subscription_key:
        return {"ok": False, "error": "Missing subscription key"}

    url = f"https://{host}{path}"
    
    # Build query parameters - only include non-empty values
    params = {}
    if group_account:
        params["groupAccount"] = group_account
    if account_number:
        params["accountNumber"] = account_number
    if fields:
        params["fields"] = fields
    if source_system:
        params["sourceSystem"] = source_system
    if output:
        params["output"] = output

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept": "application/json, text/csv",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        content_type = resp.headers.get("Content-Type", "")
        body_text = resp.text

        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type,
            "body": body_text,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def parse_intraday_trades_response(response_body: str, content_type: str) -> list:
    """
    Parse CSV or JSON response from intraday trades endpoint.
    
    Returns a list of trade dictionaries with normalized field names.
    
    API Response Fields (mapped to snake_case):
        ABSTradeValue -> abs_trade_value
        Account -> account
        AccountBaseCurrency -> account_base_currency
        AccountId -> account_id
        AssetClass -> asset_class
        ContractDescription -> contract_description
        ContractSize -> contract_size
        CounterCurrency -> counter_currency
        Currency -> currency
        CurrencyPair -> currency_pair
        Id -> rest_trade_id
        LastPx -> last_px
        LastQty -> last_qty
        ProcessDate -> process_date
        SettlementDate -> settlement_date_int
        SettlementPrice -> settlement_price
        TradeCurrency -> trade_currency
        TradeDate -> trade_date_int
        TransactTime -> transact_time
    """
    import csv
    import io
    import json
    
    trades = []
    
    # Field name mapping from API to database columns
    field_mapping = {
        "ABSTradeValue": "abs_trade_value",
        "Account": "account",
        "AccountBaseCurrency": "account_base_currency",
        "AccountId": "account_id",
        "AssetClass": "asset_class",
        "ContractDescription": "contract_description",
        "ContractSize": "contract_size",
        "CounterCurrency": "counter_currency",
        "Currency": "currency",
        "CurrencyPair": "currency_pair",
        "Id": "rest_trade_id",
        "NeoId": "neo_id",
        "TagNumber": "tag_number",
        "OrderId": "order_id",
        "ClOrdId": "clord_id",
        "LastPx": "last_px",
        "LastQty": "last_qty",
        "ProcessDate": "process_date",
        "SettlementDate": "settlement_date_int",
        "SettlementPrice": "settlement_price",
        "TradeCurrency": "trade_currency",
        "TradeDate": "trade_date_int",
        "TransactTime": "transact_time",
    }
    
    try:
        if "json" in content_type.lower():
            # Parse JSON response
            data = json.loads(response_body)
            # Data might be a list or wrapped in an object
            if isinstance(data, list):
                raw_trades = data
            elif isinstance(data, dict) and "data" in data:
                raw_trades = data["data"]
            elif isinstance(data, dict) and "trades" in data:
                raw_trades = data["trades"]
            else:
                raw_trades = [data] if data else []
            
            for raw in raw_trades:
                trade = {}
                for api_field, db_field in field_mapping.items():
                    if api_field in raw:
                        trade[db_field] = raw[api_field]
                if trade:
                    trades.append(trade)
                    
        elif "csv" in content_type.lower() or response_body.strip().startswith('"') or ',' in response_body.split('\n')[0]:
            # Parse CSV response
            reader = csv.DictReader(io.StringIO(response_body))
            for row in reader:
                trade = {}
                for api_field, db_field in field_mapping.items():
                    if api_field in row:
                        trade[db_field] = row[api_field]
                if trade:
                    trades.append(trade)
        else:
            # Try JSON as fallback
            try:
                data = json.loads(response_body)
                if isinstance(data, list):
                    for raw in data:
                        trade = {}
                        for api_field, db_field in field_mapping.items():
                            if api_field in raw:
                                trade[db_field] = raw[api_field]
                        if trade:
                            trades.append(trade)
            except:
                pass
                
    except Exception as e:
        print(f"Error parsing intraday trades: {e}")
    
    return trades


def fetch_account_balances(
    host: str,
    access_token: str,
    subscription_key: str,
    group_account: str = "",
    account_number: str = "",
    fields: str = "",
    source_system: str = "GMI,Murex,XTP,TwoFourCE",
    output: str = "csv",
    path: str = "/global-balances/eod",
) -> Dict:
    """
    GET /global-balances/eod
    Retrieves end-of-day account balances from StoneX API.
    
    Args:
        host: API host (e.g., "api.stonex.com")
        access_token: Bearer token from authentication
        subscription_key: Ocp-Apim-Subscription-Key
        group_account: Group account number (optional)
        account_number: Account number (optional)
        fields: Comma-separated list of fields (optional)
        source_system: Source systems - GMI,Murex,XTP,TwoFourCE (default: all)
        output: Response format - csv or json (default: csv)
    
    Returns:
        Dict with ok, status, body, and content_type
    """
    if not access_token:
        return {"ok": False, "error": "Missing access token"}
    if not subscription_key:
        return {"ok": False, "error": "Missing subscription key"}

    url = f"https://{host}{path}"
    
    # Build query parameters - only include non-empty values
    params = {}
    if group_account:
        params["groupAccount"] = group_account
    if account_number:
        params["accountNumber"] = account_number
    if fields:
        params["fields"] = fields
    if source_system:
        params["sourceSystem"] = source_system
    if output:
        params["output"] = output

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept": "application/json, text/csv",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        content_type = resp.headers.get("Content-Type", "")
        body_text = resp.text

        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type,
            "body": body_text,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_download_url(download_url: str) -> Dict:
    """
    Fetch a CSV file from a downloadUrl returned by StoneX endpoints.
    """
    headers = {
        "Accept": "text/csv",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }

    try:
        resp = requests.get(download_url, headers=headers, timeout=120)
        content_type = resp.headers.get("Content-Type", "")
        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "content_type": content_type,
            "body": resp.text,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_pmx_load_account(
    acccode: str,
    host: str = "pmxapi.stonex.com",
    path: str = "/user/loadAccount",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """
    Call PMX account endpoint.

    Example endpoint:
      GET https://pmxapi.stonex.com/user/loadAccount?acccode=MT0601
    """
    acccode_value = str(acccode or "").strip()
    if not acccode_value:
        return {"ok": False, "error": "Missing acccode"}

    url = path if str(path).startswith("http") else f"https://{host}{path}"
    params = {"acccode": acccode_value}
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        content_type_resp = resp.headers.get("Content-Type", "")
        body_text = resp.text if isinstance(resp.text, str) else ""

        parsed_json: Any = None
        if "json" in content_type_resp.lower() or body_text.lstrip().startswith(("{", "[")):
            try:
                parsed_json = resp.json()
            except Exception:
                parsed_json = None

        ok = bool(resp.ok)
        message = ""
        if isinstance(parsed_json, dict):
            pmx_status = str(parsed_json.get("status", "")).strip().lower()
            if pmx_status in {"failed", "error"}:
                ok = False
            message = str(parsed_json.get("message", "")).strip()

        error = ""
        if not ok:
            error = message or resp.reason or "PMX endpoint returned an error"

        return {
            "ok": ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type_resp,
            "response_headers": {
                "access-control-allow-origin": resp.headers.get("access-control-allow-origin", ""),
                "access-control-allow-credentials": resp.headers.get("access-control-allow-credentials", ""),
                "access-control-allow-headers": resp.headers.get("access-control-allow-headers", ""),
                "access-control-allow-methods": resp.headers.get("access-control-allow-methods", ""),
            },
            "body": body_text,
            "json": parsed_json,
            "error": error,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_pmx_alldeal_filter_report(
    start_date: str,
    end_date: str,
    cmdty: str = "USDZAR",
    trd_opt: str = "PMX",
    created_by: str = "2",
    acc_opt_key: str = "MT0601",
    trade_type: str = "TD",
    non_trd_cmdty: str = "",
    host: str = "pmxapi.stonex.com",
    path: str = "/user/alldealFilter_report",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """
    Call PMX all-deal filter report endpoint.

    Example endpoint:
      GET https://pmxapi.stonex.com/user/alldealFilter_report
          ?startDate=19-02-2026&endDate=19-02-2026&cmdty=USDZAR...

    Returns a dict with response metadata plus parsed JSON when available.
    """
    url = path if str(path).startswith("http") else f"https://{host}{path}"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "cmdty": cmdty,
        "Trd_opt": trd_opt,
        "created_by": created_by,
        "Acc_optKey": acc_opt_key,
        "type": trade_type,
        "nonTrdCmdty": non_trd_cmdty,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        content_type = resp.headers.get("Content-Type", "")
        body_text = resp.text if isinstance(resp.text, str) else ""

        parsed_json: Any = None
        if "json" in content_type.lower() or body_text.lstrip().startswith(("{", "[")):
            try:
                parsed_json = resp.json()
            except Exception:
                parsed_json = None

        ok = bool(resp.ok)
        message = ""
        if isinstance(parsed_json, dict):
            pmx_status = str(parsed_json.get("status", "")).strip().lower()
            if pmx_status in {"failed", "error"}:
                ok = False
            message = str(parsed_json.get("message", "")).strip()

        error = ""
        if not ok:
            error = message or resp.reason or "PMX endpoint returned an error"

        return {
            "ok": ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type,
            "response_headers": {
                "access-control-allow-origin": resp.headers.get("access-control-allow-origin", ""),
                "access-control-allow-credentials": resp.headers.get("access-control-allow-credentials", ""),
                "access-control-allow-headers": resp.headers.get("access-control-allow-headers", ""),
                "access-control-allow-methods": resp.headers.get("access-control-allow-methods", ""),
            },
            "body": body_text,
            "json": parsed_json,
            "error": error,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def fetch_pmx_account_statement_report(
    start_date: str,
    end_date: str,
    acc_code: str = "MT0601",
    report_type: str = "docDate",
    col1: str = "LC",
    col2: str = "GLD",
    unit_code1: str = "",
    unit_code2: str = "OZ",
    option: str = "1",
    host: str = "pmxapi.stonex.com",
    path: str = "/user/account_statementReport",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """Call PMX account statement report endpoint."""
    url = path if str(path).startswith("http") else f"https://{host}{path}"
    params = {
        "accCode": acc_code,
        "type": report_type,
        "startDate": start_date,
        "endDate": end_date,
        "col1": col1,
        "col2": col2,
        "unit_code1": unit_code1,
        "unit_code2": unit_code2,
        "option": option,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        content_type_resp = resp.headers.get("Content-Type", "")
        body_text = resp.text if isinstance(resp.text, str) else ""

        parsed_json: Any = None
        if body_text.strip().startswith(("{", "[")):
            try:
                parsed_json = json.loads(body_text)
            except Exception:
                parsed_json = None

        error = ""
        if not resp.ok:
            preview = body_text[:300].strip() if body_text else ""
            if preview:
                error = preview
            elif resp.reason:
                error = f"{resp.reason} (HTTP {resp.status_code})"
            else:
                error = f"PMX account statement request failed (HTTP {resp.status_code})"

        return {
            "ok": resp.ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type_resp,
            "body": body_text,
            "json": parsed_json,
            "error": error,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def extract_pmx_statement_report_rows(payload: Any, require_doc_token: bool = True) -> List[Dict[str, Any]]:
    """Decode PMX account_statementReport rows (supports nested response shapes)."""
    def _decode_json_like(value: Any, max_depth: int = 3) -> Any:
        out = value
        for _ in range(max_depth):
            if not isinstance(out, str):
                break
            text = out.strip()
            if not text.startswith(("{", "[")):
                break
            try:
                out = json.loads(text)
            except Exception:
                break
        return out

    decoded = _decode_json_like(payload)
    if decoded is None:
        return []

    rows: List[Dict[str, Any]] = []
    seen = set()
    doc_pat = re.compile(r"\b(?:FNC|JRV|JRC|MER|SWT|FCT)/\d{4}/\d+\b", re.IGNORECASE)

    def _row_to_key(row: Dict[str, Any]) -> str:
        try:
            return json.dumps(row, sort_keys=True, default=str)
        except Exception:
            return str(sorted((str(k), str(v)) for k, v in row.items()))

    def _append_row(row: Dict[str, Any]) -> None:
        if not isinstance(row, dict) or not row:
            return
        key = _row_to_key(row)
        if key in seen:
            return
        seen.add(key)
        rows.append(row)

    def _normalize_header(value: Any, idx: int) -> str:
        text = str(value or "").strip()
        if text:
            return text
        return f"col_{idx+1}"

    def _process_table_like(items: List[Any], headers_hint: Optional[List[Any]] = None) -> None:
        if not isinstance(items, list) or not items:
            return

        # Case A: list[dict]
        if all(isinstance(x, dict) for x in items):
            for x in items:
                _append_row(x)
            return

        # Case B: list[list/tuple] style tabular payload
        if not all(isinstance(x, (list, tuple)) for x in items):
            return

        rows_as_lists = [list(x) for x in items]
        if not rows_as_lists:
            return

        headers: List[str] = []
        start_idx = 0
        if headers_hint and isinstance(headers_hint, list):
            headers = [_normalize_header(h, i) for i, h in enumerate(headers_hint)]
        else:
            first = rows_as_lists[0]
            first_text = " ".join(str(v or "") for v in first).upper()
            first_is_header = (
                "DOC" in first_text or "DATE" in first_text or "NARR" in first_text or "BAL" in first_text
            ) and (doc_pat.search(first_text) is None)
            if first_is_header:
                headers = [_normalize_header(h, i) for i, h in enumerate(first)]
                start_idx = 1
            else:
                max_len = max(len(r) for r in rows_as_lists)
                headers = [f"col_{i+1}" for i in range(max_len)]

        for raw_row in rows_as_lists[start_idx:]:
            if not raw_row:
                continue
            if len(raw_row) < len(headers):
                raw_row = raw_row + [""] * (len(headers) - len(raw_row))
            mapped = {headers[i]: raw_row[i] if i < len(raw_row) else "" for i in range(len(headers))}
            _append_row(mapped)

    def _walk(node: Any) -> None:
        node = _decode_json_like(node)
        if isinstance(node, list):
            _process_table_like(node)
            for item in node:
                _walk(item)
            return
        if not isinstance(node, dict):
            return

        # Common payload envelopes where rows are list-of-dict or list-of-list.
        for rows_key in ("data", "rows", "result", "payload", "list", "table", "statement", "items"):
            if rows_key in node and isinstance(node.get(rows_key), list):
                headers_hint = None
                for hk in ("columns", "headers", "header", "cols"):
                    hv = node.get(hk)
                    if isinstance(hv, list) and hv:
                        headers_hint = hv
                        break
                _process_table_like(node.get(rows_key), headers_hint=headers_hint)

        # Candidate row if it contains any known transaction token or has row-like scalar shape.
        text = " ".join(str(v or "") for v in node.values())
        scalar_values = [v for v in node.values() if not isinstance(v, (dict, list))]
        scalar_count = len(scalar_values)
        looks_row = (bool(doc_pat.search(text)) and scalar_count > 0) or scalar_count >= 5
        if looks_row:
            _append_row(node)

        for value in node.values():
            if isinstance(value, (dict, list, str)):
                _walk(value)

    _walk(decoded)

    if not require_doc_token:
        return rows

    # Keep only candidates that reference PMX statement doc tokens.
    filtered = []
    for row in rows:
        row_text = " ".join(str(v or "") for v in row.values())
        if doc_pat.search(row_text):
            filtered.append(row)
    return filtered


def fetch_pmx_fixinvoice_pdf(
    cell: str,
    doc_type: str = "FNC",
    host: str = "pmxapi.stonex.com",
    path: str = "/user/export_FixInvoice_pdf",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """
    Download PMX Fix Invoice PDF for a support document token.

    Example:
      GET https://pmxapi.stonex.com/user/export_FixInvoice_pdf?cell=FNC/2026/048744&DocType=FNC
    """
    if not str(cell or "").strip():
        return {"ok": False, "error": "Missing cell (support document token)"}

    url = path if str(path).startswith("http") else f"https://{host}{path}"
    params = {
        "cell": cell,
        "DocType": doc_type or "FNC",
    }
    headers = {
        "Accept": "application/pdf,application/octet-stream,*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        content_type_resp = resp.headers.get("Content-Type", "")
        content_disposition = resp.headers.get("Content-Disposition", "")
        body_bytes = resp.content if isinstance(resp.content, (bytes, bytearray)) else b""
        body_text = ""
        if not body_bytes:
            body_text = resp.text if isinstance(resp.text, str) else ""
        elif "application/pdf" not in str(content_type_resp).lower():
            # Capture textual error payloads returned as bytes.
            try:
                body_text = bytes(body_bytes[:800]).decode("utf-8", errors="ignore")
            except Exception:
                body_text = ""

        is_pdf_content_type = "application/pdf" in content_type_resp.lower()
        is_pdf_disposition = "pdf" in str(content_disposition).lower()
        is_pdf_signature = body_bytes.startswith(b"%PDF")
        ok = bool(resp.ok) and (is_pdf_content_type or is_pdf_disposition or is_pdf_signature)
        error = ""
        if not ok:
            preview = body_text[:300].strip() if body_text else ""
            if preview:
                error = preview
            elif resp.reason:
                error = f"{resp.reason} (HTTP {resp.status_code})"
            else:
                error = f"PMX PDF download failed (HTTP {resp.status_code})"

        return {
            "ok": ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type_resp,
            "content_disposition": content_disposition,
            "body_bytes": bytes(body_bytes),
            "body_text": body_text,
            "error": error,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


_SUPPORT_DOC_PATTERN = re.compile(r"(?:FNC|SWT|FCT)/[^\s,;]+", re.IGNORECASE)


def extract_pmx_report_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Decode PMX all-deal payload rows.

    PMX responses are commonly shaped as:
      {"status":"success","message":"...","data":"[{...},{...}]"}
    where `data` is a JSON string containing a list of row objects.
    """
    def _decode_json_like(value: Any, max_depth: int = 3) -> Any:
        out = value
        for _ in range(max_depth):
            if not isinstance(out, str):
                break
            text = out.strip()
            if not text.startswith(("{", "[")):
                break
            try:
                out = json.loads(text)
            except Exception:
                break
        return out

    decoded = _decode_json_like(payload)
    if decoded is None:
        return []

    if isinstance(decoded, dict):
        data_blob = _decode_json_like(decoded.get("data"))
        if isinstance(data_blob, list):
            return [r for r in data_blob if isinstance(r, dict)]
        if isinstance(data_blob, dict):
            return [data_blob]
        return []

    if isinstance(decoded, list):
        return [r for r in decoded if isinstance(r, dict)]

    return []


def extract_fnc_numbers_from_pmx_report(payload: Any, cmdty: str = "USDZAR") -> List[Dict[str, str]]:
    """
    Extract support-doc tokens (FNC/SWT/FCT) from PMX report payload rows.

    The PMX schema may vary; this scanner handles nested JSON objects/lists and
    searches known fields first, then all scalar string fields.
    """
    rows = extract_pmx_report_rows(payload)
    if not rows:
        payload = payload if isinstance(payload, dict) else {}
    else:
        payload = {"data_rows": rows}

    target_cmdty = str(cmdty or "").upper().replace("/", "").replace("-", "").replace(" ", "")
    priority_fields = [
        "fnc",
        "fnc_number",
        "FNC",
        "docno",
        "DocNo",
        "doc_number",
        "DocNumber",
        "NeoId",
        "neo_id",
        "TagNumber",
        "tag_number",
        "OrderId",
        "order_id",
        "ClOrdId",
        "clord_id",
        "remarks",
        "remarks1",
        "comment",
        "notes",
        "description",
    ]

    def _norm_symbol(val: Any) -> str:
        return str(val or "").upper().replace("/", "").replace("-", "").replace(" ", "").strip()

    def _first_non_empty(record: Dict[str, Any], keys: List[str]) -> str:
        for key in keys:
            value = record.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    def _record_like(obj: Any) -> bool:
        if not isinstance(obj, dict) or not obj:
            return False
        scalar_count = sum(1 for v in obj.values() if not isinstance(v, (dict, list, tuple)))
        return scalar_count >= 2

    def _iter_records(obj: Any, seen_ids: set):
        obj_id = id(obj)
        if obj_id in seen_ids:
            return
        seen_ids.add(obj_id)

        if isinstance(obj, dict):
            if _record_like(obj):
                yield obj
            for value in obj.values():
                if isinstance(value, (dict, list, tuple)):
                    yield from _iter_records(value, seen_ids)
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                if isinstance(item, (dict, list, tuple)):
                    yield from _iter_records(item, seen_ids)

    out: List[Dict[str, str]] = []
    seen_keys = set()

    for rec in _iter_records(payload, set()):
        cmdty_value = _first_non_empty(
            rec,
            [
                "cmdty",
                "Cmdty",
                "commodity",
                "Commodity",
                "symbol",
                "Symbol",
                "CurrencyPair",
                "currency_pair",
                "instrument",
                "Instrument",
                "inst_desc",
                "instrument_desc",
                "stk_type_name",
            ],
        )
        cmdty_context = " ".join(
            [
                cmdty_value,
                _first_non_empty(rec, ["remarks", "comment", "notes", "description"]),
            ]
        )
        cmdty_norm = _norm_symbol(cmdty_context)
        if target_cmdty and target_cmdty not in cmdty_norm:
            continue

        support_doc = ""
        source_field = ""
        for field in priority_fields:
            if field not in rec:
                continue
            m = _SUPPORT_DOC_PATTERN.search(str(rec.get(field, "")))
            if m:
                support_doc = m.group(0).strip()
                source_field = field
                break

        if not support_doc:
            for key, value in rec.items():
                if isinstance(value, (dict, list, tuple)):
                    continue
                m = _SUPPORT_DOC_PATTERN.search(str(value))
                if m:
                    support_doc = m.group(0).strip()
                    source_field = str(key)
                    break

        if not support_doc:
            continue

        trade_id = _first_non_empty(
            rec,
            ["TradeId", "trade_id", "Id", "id", "RecId", "rec_id", "deal_id", "DealId", "trd"],
        )
        order_ref = _first_non_empty(
            rec,
            ["OrderId", "order_id", "ClOrdId", "clord_id", "DocNo", "docno", "doc_no", "DocNumber", "doc_number"],
        )

        dedupe_key = (support_doc, trade_id, order_ref)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        out.append(
            {
                "fnc_number": support_doc,
                "trade_id": trade_id,
                "order_ref": order_ref,
                "cmdty": cmdty_value,
                "source_field": source_field,
                "docno": _first_non_empty(rec, ["docno", "DocNo", "doc_number", "DocNumber"]),
                "deal_type": _first_non_empty(rec, ["deal_type", "DealType", "Side", "side"]),
                "event_ts": _first_non_empty(rec, ["evt_ts", "event_ts", "TransactTime", "transact_time"]),
                "remarks": _first_non_empty(rec, ["remarks", "comment", "notes", "description"]),
            }
        )

    return out


def parse_account_balances_response(response_body: str, content_type: str) -> list:
    """
    Parse CSV or JSON response from account balances endpoint.
    
    Returns a list of balance dictionaries with normalized field names.
    
    API Response Fields:
        Account, AsOfDate, AverageEquity, Cash, ClosingBalance,
        EquityExcessDeficit, FXExposureQuantity, InitialMargin,
        InitialMarginCreditLine, InitialMarginExcessDeficit,
        InitialMarginPreAdjusted, InitialMarginThreshold,
        MetalNetDelivery, MetalNetPosition, MetalSettlingTrades,
        NetLiquidationValue, NetOptionValue, OpenTradeEquity,
        OpeningBalance, TotalEquity, TotalMarginCall
    """
    import csv
    import io
    import json
    
    balances = []
    
    # Field name mapping from API to snake_case
    field_mapping = {
        "Account": "account",
        "AsOfDate": "as_of_date",
        "AverageEquity": "average_equity",
        "Cash": "cash",
        "ClosingBalance": "closing_balance",
        "EquityExcessDeficit": "equity_excess_deficit",
        "FXExposureQuantity": "fx_exposure_quantity",
        "InitialMargin": "initial_margin",
        "InitialMarginCreditLine": "initial_margin_credit_line",
        "InitialMarginExcessDeficit": "initial_margin_excess_deficit",
        "InitialMarginPreAdjusted": "initial_margin_pre_adjusted",
        "InitialMarginThreshold": "initial_margin_threshold",
        "MetalNetDelivery": "metal_net_delivery",
        "MetalNetPosition": "metal_net_position",
        "MetalSettlingTrades": "metal_settling_trades",
        "NetLiquidationValue": "net_liquidation_value",
        "NetOptionValue": "net_option_value",
        "OpenTradeEquity": "open_trade_equity",
        "OpeningBalance": "opening_balance",
        "TotalEquity": "total_equity",
        "TotalMarginCall": "total_margin_call",
    }
    
    try:
        if "json" in content_type.lower():
            # Parse JSON response
            data = json.loads(response_body)
            if isinstance(data, list):
                raw_balances = data
            elif isinstance(data, dict) and "data" in data:
                raw_balances = data["data"]
            elif isinstance(data, dict) and "balances" in data:
                raw_balances = data["balances"]
            else:
                raw_balances = [data] if data else []
            
            for raw in raw_balances:
                balance = {}
                for api_field, db_field in field_mapping.items():
                    if api_field in raw:
                        balance[db_field] = raw[api_field]
                # Also keep original field names for display
                for key, value in raw.items():
                    if key not in balance:
                        balance[key] = value
                if balance:
                    balances.append(balance)
                    
        elif "csv" in content_type.lower() or response_body.strip().startswith('"') or ',' in response_body.split('\n')[0]:
            # Parse CSV response
            reader = csv.DictReader(io.StringIO(response_body))
            for row in reader:
                balance = {}
                for api_field, db_field in field_mapping.items():
                    if api_field in row:
                        balance[db_field] = row[api_field]
                # Also keep original field names for display
                for key, value in row.items():
                    if key not in balance:
                        balance[key] = value
                if balance:
                    balances.append(balance)
        else:
            # Try JSON as fallback
            try:
                data = json.loads(response_body)
                if isinstance(data, list):
                    for raw in data:
                        balance = {}
                        for api_field, db_field in field_mapping.items():
                            if api_field in raw:
                                balance[db_field] = raw[api_field]
                        if balance:
                            balances.append(balance)
            except:
                pass
                
    except Exception as e:
        print(f"Error parsing account balances: {e}")

    return balances


def fetch_pmx_trade_activity_log(
    start_date: str,
    end_date: str,
    body_overrides: Optional[Dict[str, Any]] = None,
    host: str = "pmxapi.stonex.com",
    path: str = "/user/rptTrdActLog",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "application/json; charset=utf-8",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """POST to PMX rptTrdActLog (Trade Activity Log). Dates are DD-MM-YYYY."""
    url = path if str(path).startswith("http") else f"https://{host}{path}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    # Best-guess payload shape; content-length 43 matches {"sDate":"DD-MM-YYYY","eDate":"DD-MM-YYYY"}.
    # Fall back to alternative key shapes if PMX rejects this.
    payload_variants: List[Dict[str, Any]] = [
        {"sDate": start_date, "eDate": end_date},
        {"startDate": start_date, "endDate": end_date},
        {"fromDate": start_date, "toDate": end_date},
    ]
    if isinstance(body_overrides, dict) and body_overrides:
        payload_variants.insert(0, dict(body_overrides))

    last_result: Dict[str, Any] = {}
    for body in payload_variants:
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=timeout)
        except Exception as exc:
            last_result = {"ok": False, "error": str(exc), "request_body": body}
            continue

        content_type_resp = resp.headers.get("Content-Type", "")
        body_text = resp.text if isinstance(resp.text, str) else ""
        parsed_json: Any = None
        if "json" in content_type_resp.lower() or body_text.lstrip().startswith(("{", "[")):
            try:
                parsed_json = resp.json()
            except Exception:
                parsed_json = None

        ok = bool(resp.ok)
        message = ""
        if isinstance(parsed_json, dict):
            pmx_status = str(parsed_json.get("status", "")).strip().lower()
            if pmx_status in {"failed", "error"}:
                ok = False
            message = str(parsed_json.get("message", "")).strip()

        last_result = {
            "ok": ok,
            "status": resp.status_code,
            "reason": resp.reason,
            "url": resp.url,
            "content_type": content_type_resp,
            "body": body_text,
            "json": parsed_json,
            "error": "" if ok else (message or resp.reason or "PMX rptTrdActLog returned an error"),
            "request_body": body,
        }
        # Success or an auth/permission failure — no point trying alternate body shapes.
        if ok or resp.status_code in (401, 403):
            return last_result

    return last_result or {"ok": False, "error": "PMX rptTrdActLog request failed"}


def extract_pmx_activity_log_rows(payload: Any) -> List[Dict[str, Any]]:
    """Flexibly pull row records from the rptTrdActLog response shape."""
    def _decode_json_like(value: Any, max_depth: int = 3) -> Any:
        out = value
        for _ in range(max_depth):
            if not isinstance(out, str):
                break
            text = out.strip()
            if not text.startswith(("{", "[")):
                break
            try:
                out = json.loads(text)
            except Exception:
                break
        return out

    candidates: List[Any] = []
    payload = _decode_json_like(payload)
    if isinstance(payload, list):
        candidates.append(payload)
    elif isinstance(payload, dict):
        for key in ("data", "rows", "result", "activities", "activityLog", "tradeActivity", "records", "list"):
            val = _decode_json_like(payload.get(key))
            if isinstance(val, list):
                candidates.append(val)
            elif isinstance(val, dict):
                for sub in ("rows", "list", "data", "records", "items"):
                    inner = _decode_json_like(val.get(sub))
                    if isinstance(inner, list):
                        candidates.append(inner)
        if not candidates:
            for val in payload.values():
                val = _decode_json_like(val)
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    candidates.append(val)

    rows: List[Dict[str, Any]] = []
    for group in candidates:
        for rec in group:
            if isinstance(rec, dict):
                rows.append(rec)
    return rows


def fetch_pmx_online_report(
    start_date: str,
    end_date: str,
    cmdty: str = "",
    order_opt: str = "O",
    event_time: str = "",
    acc_opt: str = "MT0601",
    creatby_opt: str = "2",
    host: str = "pmxapi.stonex.com",
    path: str = "/user/online_Report",
    authorization: str = "",
    cookie: str = "",
    x_auth: str = "",
    sid: str = "",
    username: str = "",
    platform: str = "",
    location: str = "",
    cache_control: str = "",
    content_type: str = "application/json; charset=utf-8",
    extra_headers: Optional[Dict[str, str]] = None,
    origin: str = "https://pmxecute.stonex.com",
    referer: str = "https://pmxecute.stonex.com/",
    timeout: int = 60,
) -> Dict:
    """GET PMX online report endpoint (open/pending orders view)."""
    url = path if str(path).startswith("http") else f"https://{host}{path}"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "Cmdty": cmdty or "",
        "Order_opt": order_opt or "O",
        "EventTime": event_time or "",
        "Acc_opt": acc_opt or "MT0601",
        "creatby_opt": creatby_opt or "2",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    }
    if origin:
        headers["Origin"] = origin
    if referer:
        headers["Referer"] = referer
    if authorization:
        headers["Authorization"] = authorization
    if cookie:
        headers["Cookie"] = cookie
    if x_auth:
        headers["x-auth"] = x_auth
    if sid:
        headers["sid"] = sid
    if username:
        headers["username"] = username
    if platform:
        headers["platform"] = platform
    if location:
        headers["location"] = location
    if cache_control:
        headers["cache-control"] = cache_control
    if content_type:
        headers["content-type"] = content_type
    if isinstance(extra_headers, dict):
        for key, value in extra_headers.items():
            if key and value is not None and str(value).strip():
                headers[str(key)] = str(value)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    content_type_resp = resp.headers.get("Content-Type", "")
    body_text = resp.text if isinstance(resp.text, str) else ""
    parsed_json: Any = None
    if "json" in content_type_resp.lower() or body_text.lstrip().startswith(("{", "[")):
        try:
            parsed_json = resp.json()
        except Exception:
            parsed_json = None

    ok = bool(resp.ok)
    message = ""
    if isinstance(parsed_json, dict):
        pmx_status = str(parsed_json.get("status", "")).strip().lower()
        if pmx_status in {"failed", "error"}:
            ok = False
        message = str(parsed_json.get("message", "")).strip()

    return {
        "ok": ok,
        "status": resp.status_code,
        "reason": resp.reason,
        "url": resp.url,
        "content_type": content_type_resp,
        "body": body_text,
        "json": parsed_json,
        "error": "" if ok else (message or resp.reason or "PMX online_Report returned an error"),
        "request_params": params,
    }


def extract_pmx_online_report_rows(payload: Any) -> List[Dict[str, Any]]:
    """Flexibly pull row records from PMX online_Report response shape."""
    def _decode_json_like(value: Any, max_depth: int = 3) -> Any:
        out = value
        for _ in range(max_depth):
            if not isinstance(out, str):
                break
            text = out.strip()
            if not text.startswith(("{", "[")):
                break
            try:
                out = json.loads(text)
            except Exception:
                break
        return out

    candidates: List[Any] = []
    payload = _decode_json_like(payload)
    if isinstance(payload, list):
        candidates.append(payload)
    elif isinstance(payload, dict):
        for key in ("data", "rows", "result", "list", "records", "items", "onlineReport", "report"):
            val = _decode_json_like(payload.get(key))
            if isinstance(val, list):
                candidates.append(val)
            elif isinstance(val, dict):
                for sub in ("rows", "list", "data", "records", "items"):
                    inner = _decode_json_like(val.get(sub))
                    if isinstance(inner, list):
                        candidates.append(inner)
        if not candidates:
            for val in payload.values():
                val = _decode_json_like(val)
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    candidates.append(val)

    rows: List[Dict[str, Any]] = []
    for group in candidates:
        for rec in group:
            if isinstance(rec, dict):
                rows.append(rec)
    return rows
