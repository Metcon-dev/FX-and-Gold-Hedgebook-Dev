"""
Query StoneX EOD Metal Balances for all metals.

Uses the /global-balances/eod endpoint to retrieve metal balance data
for the configured group account across all source systems.
"""
import sys
import os
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import STONEX_CONFIG
from services.rest_service import login_get_token, fetch_account_balances, fetch_download_url

def load_env_file():
    """Manually load .env file since python-dotenv may not be installed."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars

def main():
    env_vars = load_env_file()
    
    cfg = {
        "host": env_vars.get("STONEX_API_HOST", STONEX_CONFIG["host"]),
        "subscription_key": env_vars.get("STONEX_SUBSCRIPTION_KEY", STONEX_CONFIG["subscription_key"]),
        "username": env_vars.get("STONEX_USERNAME", STONEX_CONFIG["username"]),
        "password": env_vars.get("STONEX_PASSWORD", STONEX_CONFIG["password"]),
    }

    # ── 1. Authenticate ──────────────────────────────────────────────
    print("Authenticating with StoneX API...")
    auth = login_get_token(
        host=cfg["host"],
        subscription_key=cfg["subscription_key"],
        username=cfg["username"],
        password=cfg["password"],
    )

    if not auth["ok"]:
        print(f"Authentication failed: {auth.get('error', 'Unknown error')}")
        print(f"Details: {json.dumps(auth, indent=2, default=str)}")
        return

    access_token = auth["access_token"]
    print(f"Authenticated successfully. Token: {access_token[:30]}...\n")

    # ── 2. Fetch EOD Balances ────────────────────────────────────────
    # Fetch all fields — the API fields filter can cause empty results,
    # so we retrieve everything and filter client-side for metal data.
    print("Fetching EOD metal balances...")
    result = fetch_account_balances(
        host=cfg["host"],
        access_token=access_token,
        subscription_key=cfg["subscription_key"],
        group_account="LPGMT0601",       # Metal Concentrators group account
        source_system="Murex",
        output="json",
    )

    if not result["ok"]:
        print(f"API call failed: {result.get('error', result.get('reason', 'Unknown'))}")
        print(f"Status: {result.get('status')}")
        print(f"URL: {result.get('url')}")
        print(f"Body (first 1000 chars): {result.get('body', '')[:1000]}")
        return

    print(f"Status: {result['status']}")
    print(f"Content-Type: {result['content_type']}")
    print(f"URL: {result['url']}")

    body = result["body"]

    # ── 3. Handle downloadUrl pattern ────────────────────────────────
    # StoneX returns a JSON wrapper with a downloadUrl instead of inline data.
    # The download endpoint returns application/octet-stream regardless of format,
    # so we detect the content based on the URL or body content.
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict) and "downloadUrl" in parsed:
            download_url = parsed["downloadUrl"]
            total_items = parsed.get("totalItems", "?")
            print(f"\nGot downloadUrl ({total_items} items), fetching...")
            dl_result = fetch_download_url(download_url)
            if dl_result.get("ok"):
                body = dl_result["body"]
                # Force correct content-type: download endpoint returns octet-stream
                # but the actual content is JSON or CSV depending on our request
                if body.strip().startswith("{") or body.strip().startswith("["):
                    result["content_type"] = "application/json"
                else:
                    result["content_type"] = "text/csv"
            else:
                print(f"Failed to download: {dl_result.get('error')}")
                return
    except (json.JSONDecodeError, TypeError):
        pass

    # ── 4. Parse & display results ───────────────────────────────────
    # The StoneX download endpoint returns NDJSON (one JSON object per line),
    # not a JSON array, so we parse line by line.
    balances = []
    for line in body.strip().splitlines():
        line = line.strip()
        if line:
            try:
                balances.append(json.loads(line))
            except json.JSONDecodeError:
                pass  # skip non-JSON lines (headers, etc.)
    
    if not balances:
        print("\nNo balance records returned.")
        print(f"Raw response (first 2000 chars):\n{body[:2000]}")
        return

    print(f"\n{'='*80}")
    print(f"  EOD METAL BALANCES -- {len(balances)} records")
    print(f"{'='*80}\n")

    # Group by metal
    metals = {}
    for b in balances:
        metal = b.get("Metal", b.get("metal")) or "N/A"
        if metal not in metals:
            metals[metal] = []
        metals[metal].append(b)

    for metal, records in sorted(metals.items(), key=lambda x: x[0] or ""):
        print(f"\n{'-'*60}")
        print(f"  METAL: {metal}  ({len(records)} records)")
        print(f"{'-'*60}")

        for rec in records:
            acct = rec.get("AccountNumber", rec.get("account_number", rec.get("Account", rec.get("account", "N/A"))))
            acct_name = rec.get("AccountName", rec.get("account_name", ""))
            src_sys = rec.get("SourceSystem", rec.get("source_system", rec.get("AccountSourceSystem", "")))
            as_of = rec.get("AsOfDate", rec.get("as_of_date", ""))
            net_pos = rec.get("MetalNetPosition", rec.get("metal_net_position", "N/A"))
            net_del = rec.get("MetalNetDelivery", rec.get("metal_net_delivery", "N/A"))
            settling = rec.get("MetalSettlingTrades", rec.get("metal_settling_trades", "N/A"))
            closing = rec.get("ClosingBalance", rec.get("closing_balance", "N/A"))
            closing_ftoz = rec.get("ClosingBalanceFTOz", rec.get("closing_balance_ftoz", "N/A"))
            open_ftoz = rec.get("OpenBalanceFTOz", rec.get("open_balance_ftoz", "N/A"))
            open_pos_ftoz = rec.get("OpenPositionFTOz", rec.get("open_position_ftoz", "N/A"))
            currency = rec.get("TradeCurrency", rec.get("trade_currency", ""))
            cash = rec.get("Cash", rec.get("cash", "N/A"))

            print(f"  Account:  {acct}  {acct_name}")
            print(f"  Source:   {src_sys}    As Of: {as_of}    Currency: {currency}")
            print(f"  Net Position:        {net_pos}")
            print(f"  Net Delivery:        {net_del}")
            print(f"  Settling Trades:     {settling}")
            print(f"  Cash:                {cash}")
            print(f"  Closing Balance:     {closing}")
            print(f"  Closing Balance FTOz:{closing_ftoz}")
            print(f"  Open Balance FTOz:   {open_ftoz}")
            print(f"  Open Position FTOz:  {open_pos_ftoz}")
            print()

    # Also dump full JSON for inspection
    output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metal_balances_output.json")
    with open(output_file, "w") as f:
        json.dump(balances, f, indent=2, default=str)
    print(f"\nFull data saved to: {output_file}")


if __name__ == "__main__":
    main()
