"""
Backtest: Purchases Forecast Model
===================================
Fetches ALL TradeMC history directly from the API (13k+ trades since 2020),
then replays the MC supplier-payment model against actual historical months.

For each test month M:
  - Uses all TradeMC trade data BEFORE month M (up to 35 months back) as training
  - Runs the same MC forecast logic used in PurchasesForecastPanel
  - Compares predicted P10/P50/P90 total monthly payment vs actual
  - Reports accuracy metrics
"""

import requests as http_req
import math
import random
import statistics
import time
import json
from datetime import datetime, timedelta
from collections import defaultdict

# ---------- Config ----------
TRADEMC_BASE = "https://trademc-admin.metcon.co.za"
TRADEMC_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"
GRAMS_PER_TROY_OUNCE = 31.1035
SIM_PATHS = 2000
TEST_MONTHS = 12  # backtest the last 12 complete months

# ---------- Fetch ALL trades from TradeMC API ----------
print("Fetching all TradeMC trades from API...")
headers = {"Authorization": f"Bearer {TRADEMC_KEY}", "Content-Type": "application/json"}

all_trades = []
last_id = 0
page_size = 100

while True:
    params = {
        "limit": page_size,
        "offset": 0,
        "sort": "id",
        "filter[id][_gt]": last_id,
    }
    if last_id == 0:
        params["meta"] = "total_count"

    r = http_req.get(f"{TRADEMC_BASE}/items/trade", headers=headers, params=params, timeout=45)
    if r.status_code != 200:
        print(f"  API error {r.status_code}: {r.text[:200]}")
        break

    body = r.json()
    if last_id == 0:
        total = (body.get("meta") or {}).get("total_count", "?")
        print(f"  Total trades in API: {total}")

    batch = body.get("data", [])
    if not batch:
        break

    all_trades.extend(batch)
    ids = [int(t["id"]) for t in batch if t.get("id")]
    last_id = max(ids)

    if len(all_trades) % 2000 < page_size:
        print(f"  Fetched {len(all_trades)} trades so far (last_id={last_id})...")

    if len(batch) < page_size:
        break

print(f"  Done: {len(all_trades)} total trades fetched")

# Also fetch companies for refining rates
print("Fetching companies...")
r = http_req.get(f"{TRADEMC_BASE}/items/company", headers=headers, params={"limit": 500}, timeout=15)
companies = {}
if r.status_code == 200:
    for c in r.json().get("data", []):
        companies[c.get("id")] = c
    print(f"  {len(companies)} companies loaded")

# ---------- Helpers ----------
def parse_num(value):
    if value is None or value == '':
        return None
    try:
        n = float(str(value).replace(',', '').strip())
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None

def to_ts(value):
    if value is None or value == '':
        return None
    s = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.000Z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:min(len(s), 26)].rstrip("Z"), fmt.rstrip("Z"))
        except ValueError:
            continue
    return None

def quantile(arr, q):
    if not arr:
        return 0
    s = sorted(arr)
    pos = (len(s) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    w = pos - lo
    return s[lo] * (1 - w) + s[hi] * w

def mean(arr):
    return sum(arr) / len(arr) if arr else 0

def std(arr):
    if len(arr) < 2:
        return 0
    m = mean(arr)
    return math.sqrt(sum((v - m) ** 2 for v in arr) / (len(arr) - 1))

def median(arr):
    return quantile(arr, 0.5)

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def lin_reg(y_vals):
    n = len(y_vals)
    if n < 2:
        return (y_vals[0] if y_vals else 0), 0
    sx = sy = sxx = sxy = 0
    for i, y in enumerate(y_vals):
        sx += i; sy += y; sxx += i * i; sxy += i * y
    den = n * sxx - sx * sx
    if abs(den) < 1e-12:
        return sy / n, 0
    slope = (n * sxy - sx * sy) / den
    intercept = (sy - slope * sx) / n
    return intercept, slope

# ---------- Parse all trades into daily buckets ----------
print("\nParsing trades into daily buckets...")
daily_data = {}

for row in all_trades:
    weight = parse_num(row.get('weight'))
    if weight is None or weight <= 0:
        continue
    if str(row.get('status', '')).lower() not in ('confirmed', ''):
        continue

    dt = to_ts(row.get('trade_timestamp') or row.get('date_created'))
    if dt is None:
        continue

    day = dt.day
    date_key = dt.strftime("%Y-%m-%d")

    # Get company refining rate
    company_id = row.get('company_id') or row.get('company')
    company = companies.get(company_id, {}) if company_id else {}
    ref_rate = parse_num(company.get('refining_rate')) or 0

    usd = parse_num(row.get('usd_per_troy_ounce_confirmed') or row.get('usd_per_troy_ounce'))
    fx = parse_num(row.get('zar_to_usd_confirmed') or row.get('zar_to_usd'))
    if usd is None:
        zar_oz = parse_num(row.get('zar_per_troy_ounce_confirmed') or row.get('zar_per_troy_ounce'))
        if zar_oz is not None and fx is not None and abs(fx) > 1e-9:
            usd = zar_oz / fx

    zar_per_gram = (usd * fx / GRAMS_PER_TROY_OUNCE) if (usd is not None and fx is not None) else None
    zar_less_ref = zar_per_gram * (1 - ref_rate / 100) if zar_per_gram is not None else None
    if zar_less_ref is None:
        continue
    payment = weight * zar_less_ref

    refine_factor = 1.0
    if usd is not None and fx is not None:
        base_zpg = (usd * fx) / GRAMS_PER_TROY_OUNCE
        if base_zpg > 1e-9:
            refine_factor = clamp(zar_less_ref / base_zpg, 0.5, 1.2)

    d = daily_data.get(date_key)
    if d is None:
        d = {'day': day, 'weight': 0, 'payment': 0,
             'price_w': 0, 'price_wt': 0, 'fx_w': 0, 'fx_wt': 0,
             'refine_w': 0, 'refine_wt': 0}
        daily_data[date_key] = d

    d['weight'] += weight
    d['payment'] += payment
    if usd is not None:
        d['price_w'] += usd * weight
        d['price_wt'] += weight
    if fx is not None:
        d['fx_w'] += fx * weight
        d['fx_wt'] += weight
    d['refine_w'] += refine_factor * weight
    d['refine_wt'] += weight

# Convert to sorted list
all_daily = []
for date_key, d in sorted(daily_data.items()):
    price = d['price_w'] / d['price_wt'] if d['price_wt'] > 0 else float('nan')
    fx_val = d['fx_w'] / d['fx_wt'] if d['fx_wt'] > 0 else float('nan')
    refine = d['refine_w'] / d['refine_wt'] if d['refine_wt'] > 0 else 1
    all_daily.append({
        'date': date_key, 'day': d['day'], 'weight': d['weight'],
        'payment': d['payment'], 'price': price, 'fx': fx_val, 'refine_factor': refine,
    })

print(f"  {len(all_daily)} unique trade days from {all_daily[0]['date']} to {all_daily[-1]['date']}")

# ---------- Monthly actuals ----------
month_totals = defaultdict(lambda: {'weight': 0, 'payment': 0, 'days': 0})
for d in all_daily:
    m_key = d['date'][:7]
    month_totals[m_key]['weight'] += d['weight']
    month_totals[m_key]['payment'] += d['payment']
    month_totals[m_key]['days'] += 1

all_months = sorted(month_totals.keys())
print(f"  {len(all_months)} months with data: {all_months[0]} to {all_months[-1]}")

# Print monthly data summary
print(f"\n  Monthly History:")
print(f"  {'MONTH':<10} {'DAYS':>6} {'WEIGHT (g)':>14} {'PAYMENT (ZAR)':>18}")
print(f"  {'-'*52}")
for m in all_months:
    mt = month_totals[m]
    print(f"  {m:<10} {mt['days']:>6} {mt['weight']:>14,.0f} R {mt['payment']:>15,.0f}")

# Test months: last N complete months (exclude current partial month)
now = datetime.now()
current_month = now.strftime("%Y-%m")
test_candidates = [m for m in all_months if m < current_month]
# Need at least 12 months of history before a test month
test_months = test_candidates[-TEST_MONTHS:]
print(f"\n  Backtesting {len(test_months)} months: {test_months[0]} to {test_months[-1]}")

# ---------- Forecast function ----------
def run_forecast_for_month(target_month_str, history_daily):
    year = int(target_month_str[:4])
    month = int(target_month_str[5:7])
    days_in_month = ((datetime(year + (1 if month == 12 else 0),
                               (month % 12) + 1, 1)) - datetime(year, month, 1)).days

    if len(history_daily) < 10:
        return None

    prices = [d['price'] for d in history_daily if math.isfinite(d['price']) and d['price'] > 0]
    fx_vals = [d['fx'] for d in history_daily if math.isfinite(d['fx']) and d['fx'] > 0]
    refine_vals = [d['refine_factor'] for d in history_daily if d['refine_factor'] > 0]

    median_fx = median(fx_vals) if fx_vals else 18
    median_refine = median(refine_vals) if refine_vals else 1
    median_price = median(prices) if prices else 3000

    if len(prices) < 5:
        prices = []
        for d in history_daily:
            if d['weight'] > 0 and d['payment'] > 0:
                p = (d['payment'] / d['weight'] * GRAMS_PER_TROY_OUNCE) / max(1e-9, median_fx * median_refine)
                if math.isfinite(p) and p > 0:
                    prices.append(p)
        median_price = median(prices) if prices else 3000

    # Median baseline (stable, no look-ahead)
    def get_base_price(_):
        return median_price
    base_fx = median_fx

    # Monthly weight target from last 12 months of history
    monthly_map = defaultdict(float)
    for d in history_daily:
        monthly_map[d['date'][:7]] += d['weight']
    monthly_weights = list(monthly_map.values())
    monthly_weight_target = mean(monthly_weights[-12:]) if len(monthly_weights) >= 3 else mean(monthly_weights) if monthly_weights else 1000

    # Day-of-month profile
    daily_profile = [{'weight': 0, 'count': 0} for _ in range(31)]
    for d in history_daily:
        day = d['day']
        daily_profile[day - 1]['weight'] += d['weight']
        daily_profile[day - 1]['count'] += 1

    profile_raw = [(daily_profile[i]['weight'] / daily_profile[i]['count']
                     if daily_profile[i]['count'] > 0 else 0)
                    for i in range(days_in_month)]
    profile_smoothed = list(profile_raw)
    for i, v in enumerate(profile_raw):
        if v <= 0:
            prev = profile_raw[i - 1] if i > 0 else 0
            nxt = profile_raw[i + 1] if i < len(profile_raw) - 1 else 0
            profile_smoothed[i] = (prev + nxt) / 2
    smoothed_base = sum(profile_smoothed)
    if smoothed_base > 1e-9:
        profile_shares = [v / smoothed_base for v in profile_smoothed]
    else:
        profile_shares = [1 / days_in_month] * days_in_month

    # Price/weight correlation
    price_returns = []
    weight_returns = []
    for i in range(1, len(history_daily)):
        p0 = max(1e-6, history_daily[i - 1]['price'])
        p1 = max(1e-6, history_daily[i]['price'])
        w0 = max(1, history_daily[i - 1]['weight'])
        w1 = max(1, history_daily[i]['weight'])
        if math.isfinite(p0) and math.isfinite(p1):
            price_returns.append(math.log(p1 / p0))
            weight_returns.append(math.log(w1 / w0))

    p_mean = mean(price_returns) if price_returns else 0
    w_mean = mean(weight_returns) if weight_returns else 0
    cov = pvar = 0
    for i in range(len(price_returns)):
        cov += (price_returns[i] - p_mean) * (weight_returns[i] - w_mean)
        pvar += (price_returns[i] - p_mean) ** 2
    beta = clamp(cov / pvar, -2.5, 2.5) if pvar > 1e-12 else -0.2
    sigma_p = clamp(std(price_returns), 0.0025, 0.08)
    resid = [weight_returns[i] - beta * price_returns[i] for i in range(len(price_returns))]
    sigma_w = clamp(std(resid), 0.01, 0.35)

    # Scenarios
    scenarios = [
        {'label': 'Down 15%', 'shift': -0.15},
        {'label': 'Down 10%', 'shift': -0.10},
        {'label': 'Down 5%', 'shift': -0.05},
        {'label': 'Baseline', 'shift': 0},
        {'label': 'Up 5%', 'shift': 0.05},
        {'label': 'Up 10%', 'shift': 0.10},
        {'label': 'Up 15%', 'shift': 0.15},
    ]

    results = {}
    for sc in scenarios:
        total_payments = []
        for p in range(SIM_PATHS):
            total = 0
            prev_price = max(1, get_base_price(0) * (1 + sc['shift']))
            for d in range(days_in_month):
                base_price = max(1, get_base_price(d) * (1 + sc['shift']))
                shock = random.gauss(0, 1) * sigma_p
                step_price = max(1, prev_price * math.exp(shock))
                price = 0.65 * step_price + 0.35 * base_price
                rel = price / max(1e-6, get_base_price(d))
                base_day_weight = monthly_weight_target * profile_shares[d]
                weight = max(0, base_day_weight * math.exp(beta * math.log(max(rel, 1e-6)) + random.gauss(0, 1) * sigma_w))
                zar_per_gram_less_ref = (price * base_fx / GRAMS_PER_TROY_OUNCE) * median_refine
                payment = max(0, weight * zar_per_gram_less_ref)
                total += payment
                prev_price = price
            total_payments.append(total)

        results[sc['label']] = {
            'p10': quantile(total_payments, 0.10),
            'p50': quantile(total_payments, 0.50),
            'p90': quantile(total_payments, 0.90),
        }

    return results


# ---------- Execute backtest ----------
print("\n" + "=" * 110)
print(f"{'MONTH':<10} {'ACTUAL (ZAR)':>18} {'PRED P50 (ZAR)':>18} {'P50 ERR %':>12} {'PRED P10':>18} {'PRED P90':>18} {'IN P10-P90':>12}")
print("=" * 110)

errors = []
inside_band = 0
all_results = []

for test_month in test_months:
    actual = month_totals[test_month]['payment']

    # Training window: up to 35 months before target month
    test_start = datetime.strptime(test_month + "-01", "%Y-%m-%d")
    window_start = test_start - timedelta(days=35 * 30)
    window_start_str = window_start.strftime("%Y-%m-%d")

    history = [d for d in all_daily
               if d['date'] < test_month + "-01" and d['date'] >= window_start_str]

    if len(history) < 10:
        print(f"{test_month:<10} {'SKIP - insufficient history':>60}")
        continue

    random.seed(hash(test_month) & 0xFFFFFFFF)

    result = run_forecast_for_month(test_month, history)
    if result is None:
        print(f"{test_month:<10} {'SKIP - forecast failed':>60}")
        continue

    baseline = result['Baseline']
    p50 = baseline['p50']
    p10 = baseline['p10']
    p90 = baseline['p90']
    err_pct = ((p50 - actual) / actual * 100) if actual > 0 else float('nan')
    in_band = p10 <= actual <= p90
    if in_band:
        inside_band += 1
    errors.append(abs(err_pct))
    all_results.append((test_month, actual, result))

    print(f"{test_month:<10} "
          f"R {actual:>15,.0f} "
          f"R {p50:>15,.0f} "
          f"{err_pct:>+10.1f}% "
          f"R {p10:>15,.0f} "
          f"R {p90:>15,.0f} "
          f"{'  YES' if in_band else '   NO':>12}")

print("=" * 110)

# ---------- Summary ----------
if errors:
    med_err = median(errors)
    mean_err = mean(errors)
    coverage = inside_band / len(errors) * 100
    print(f"\n  SUMMARY ({len(errors)} months tested):")
    print(f"  -----------------------------------------")
    print(f"  Median Absolute % Error (P50):   {med_err:.1f}%")
    print(f"  Mean Absolute % Error (P50):     {mean_err:.1f}%")
    print(f"  P10-P90 Coverage:                {inside_band}/{len(errors)} = {coverage:.0f}%")
    print(f"  (Ideal coverage for 80% CI is ~80%)")
    print()

    # Best-fit scenario per month
    print(f"  BEST-FIT SCENARIO PER MONTH:")
    print(f"  {'MONTH':<10} {'ACTUAL':>18} {'BEST SCENARIO':<16} {'BEST P50':>18} {'ERR %':>10}")
    print(f"  {'-'*76}")
    for test_month, actual, result in all_results:
        best_sc = min(result.items(), key=lambda kv: abs(kv[1]['p50'] - actual))
        best_err = (best_sc[1]['p50'] - actual) / actual * 100 if actual > 0 else float('nan')
        print(f"  {test_month:<10} R {actual:>15,.0f} {best_sc[0]:<16} R {best_sc[1]['p50']:>15,.0f} {best_err:>+8.1f}%")

    # Direction accuracy: did payment go up or down vs previous month?
    print(f"\n  DIRECTION ACCURACY (month-over-month):")
    correct_dir = 0
    total_dir = 0
    for i in range(1, len(all_results)):
        prev_actual = all_results[i-1][1]
        curr_actual = all_results[i][1]
        curr_p50 = all_results[i][2]['Baseline']['p50']
        actual_dir = "up" if curr_actual > prev_actual else "down"
        pred_dir = "up" if curr_p50 > prev_actual else "down"
        match = actual_dir == pred_dir
        if match:
            correct_dir += 1
        total_dir += 1
    if total_dir:
        print(f"  {correct_dir}/{total_dir} = {correct_dir/total_dir*100:.0f}% correct direction calls")

print("\nBacktest complete.")
