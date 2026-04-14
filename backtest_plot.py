"""
Backtest Plot: Predicted vs Actual Purchase Order Amounts (Last 6 Months)
=========================================================================
Fetches TradeMC data, runs MC forecast for last 6 complete months,
then plots predicted P50 vs actual monthly totals with P10-P90 bands.
"""

import requests as http_req
import math
import random
import os
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("ERROR: matplotlib is required. Install with: pip install matplotlib")
    exit(1)

# ---------- Config ----------
TRADEMC_BASE = "https://trademc-admin.metcon.co.za"
TRADEMC_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"
GRAMS_PER_TROY_OUNCE = 31.1035
SIM_PATHS = 2000
TEST_MONTHS = 6

# ---------- Fetch ALL trades ----------
print("Fetching all TradeMC trades from API...")
headers = {"Authorization": f"Bearer {TRADEMC_KEY}", "Content-Type": "application/json"}

all_trades = []
last_id = 0
page_size = 100

while True:
    params = {"limit": page_size, "offset": 0, "sort": "id", "filter[id][_gt]": last_id}
    r = http_req.get(f"{TRADEMC_BASE}/items/trade", headers=headers, params=params, timeout=45)
    if r.status_code != 200:
        print(f"  API error {r.status_code}")
        break
    batch = r.json().get("data", [])
    if not batch:
        break
    all_trades.extend(batch)
    last_id = max(int(t["id"]) for t in batch if t.get("id"))
    if len(all_trades) % 2000 < page_size:
        print(f"  Fetched {len(all_trades)} trades...")
    if len(batch) < page_size:
        break

print(f"  Done: {len(all_trades)} total trades")

# Fetch companies
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

# ---------- Parse trades into daily buckets ----------
print("Parsing trades into daily buckets...")
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

all_daily = []
for date_key, d in sorted(daily_data.items()):
    price = d['price_w'] / d['price_wt'] if d['price_wt'] > 0 else float('nan')
    fx_val = d['fx_w'] / d['fx_wt'] if d['fx_wt'] > 0 else float('nan')
    refine = d['refine_w'] / d['refine_wt'] if d['refine_wt'] > 0 else 1
    all_daily.append({
        'date': date_key, 'day': d['day'], 'weight': d['weight'],
        'payment': d['payment'], 'price': price, 'fx': fx_val, 'refine_factor': refine,
    })

print(f"  {len(all_daily)} trade days: {all_daily[0]['date']} to {all_daily[-1]['date']}")

# Monthly actuals
month_totals = defaultdict(lambda: {'weight': 0, 'payment': 0, 'days': 0})
for d in all_daily:
    m_key = d['date'][:7]
    month_totals[m_key]['weight'] += d['weight']
    month_totals[m_key]['payment'] += d['payment']
    month_totals[m_key]['days'] += 1

all_months = sorted(month_totals.keys())
now = datetime.now()
current_month = now.strftime("%Y-%m")
test_candidates = [m for m in all_months if m < current_month]
test_months = test_candidates[-TEST_MONTHS:]
print(f"  Backtesting {len(test_months)} months: {test_months[0]} to {test_months[-1]}")

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

    def get_base_price(_):
        return median_price
    base_fx = median_fx

    monthly_map = defaultdict(float)
    for d in history_daily:
        monthly_map[d['date'][:7]] += d['weight']
    monthly_weights = list(monthly_map.values())
    monthly_weight_target = mean(monthly_weights[-12:]) if len(monthly_weights) >= 3 else mean(monthly_weights) if monthly_weights else 1000

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

    total_payments = []
    for p in range(SIM_PATHS):
        total = 0
        prev_price = max(1, get_base_price(0))
        for d in range(days_in_month):
            base_price = max(1, get_base_price(d))
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

    return {
        'p10': quantile(total_payments, 0.10),
        'p25': quantile(total_payments, 0.25),
        'p50': quantile(total_payments, 0.50),
        'p75': quantile(total_payments, 0.75),
        'p90': quantile(total_payments, 0.90),
    }


# ---------- Run backtest ----------
print("\nRunning backtest...")
results = []

for test_month in test_months:
    actual = month_totals[test_month]['payment']

    test_start = datetime.strptime(test_month + "-01", "%Y-%m-%d")
    window_start = test_start - timedelta(days=35 * 30)
    window_start_str = window_start.strftime("%Y-%m-%d")

    history = [d for d in all_daily
               if d['date'] < test_month + "-01" and d['date'] >= window_start_str]

    if len(history) < 10:
        print(f"  {test_month}: SKIP - insufficient history")
        continue

    random.seed(hash(test_month) & 0xFFFFFFFF)

    forecast = run_forecast_for_month(test_month, history)
    if forecast is None:
        print(f"  {test_month}: SKIP - forecast failed")
        continue

    err_pct = ((forecast['p50'] - actual) / actual * 100) if actual > 0 else 0
    in_band = forecast['p10'] <= actual <= forecast['p90']
    print(f"  {test_month}: Actual R{actual:,.0f}  |  Pred P50 R{forecast['p50']:,.0f}  |  Err {err_pct:+.1f}%  |  In P10-P90: {'YES' if in_band else 'NO'}")

    results.append({
        'month': test_month,
        'actual': actual,
        'p10': forecast['p10'],
        'p25': forecast['p25'],
        'p50': forecast['p50'],
        'p75': forecast['p75'],
        'p90': forecast['p90'],
    })

if not results:
    print("No results to plot!")
    exit(1)

# ---------- Plot ----------
print("\nGenerating plot...")

months = [r['month'] for r in results]
actuals = [r['actual'] for r in results]
p50s = [r['p50'] for r in results]
p10s = [r['p10'] for r in results]
p25s = [r['p25'] for r in results]
p75s = [r['p75'] for r in results]
p90s = [r['p90'] for r in results]

x = range(len(months))

fig, ax = plt.subplots(figsize=(14, 7))

# Confidence bands
ax.fill_between(x, p10s, p90s, alpha=0.15, color='#2196F3', label='P10-P90 band')
ax.fill_between(x, p25s, p75s, alpha=0.25, color='#2196F3', label='P25-P75 band')

# Lines
ax.plot(x, actuals, 'o-', color='#E53935', linewidth=2.5, markersize=10, label='Actual', zorder=5)
ax.plot(x, p50s, 's--', color='#1565C0', linewidth=2.5, markersize=10, label='Predicted (P50)', zorder=5)

# Annotate values
for i in x:
    ax.annotate(f'R{actuals[i]/1e6:.1f}M', (i, actuals[i]),
                textcoords="offset points", xytext=(0, 14), ha='center',
                fontsize=9, color='#E53935', fontweight='bold')
    ax.annotate(f'R{p50s[i]/1e6:.1f}M', (i, p50s[i]),
                textcoords="offset points", xytext=(0, -18), ha='center',
                fontsize=9, color='#1565C0', fontweight='bold')

# Error % labels
for i in x:
    err = (p50s[i] - actuals[i]) / actuals[i] * 100 if actuals[i] > 0 else 0
    mid = (actuals[i] + p50s[i]) / 2
    ax.annotate(f'{err:+.1f}%', (i, mid),
                textcoords="offset points", xytext=(30, 0), ha='left',
                fontsize=8, color='#555', fontstyle='italic',
                arrowprops=dict(arrowstyle='->', color='#aaa', lw=0.8))

ax.set_xticks(list(x))
ax.set_xticklabels(months, fontsize=11, fontweight='bold')
ax.set_xlabel('Month', fontsize=12, fontweight='bold')
ax.set_ylabel('Monthly Purchase Amount (ZAR)', fontsize=12, fontweight='bold')
ax.set_title('Backtest: Predicted vs Actual Purchase Orders (Last 6 Months)', fontsize=15, fontweight='bold', pad=15)

ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'R{v/1e6:.0f}M' if v >= 1e6 else f'R{v/1e3:.0f}K'))
ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
ax.grid(True, alpha=0.3, linestyle='--')
ax.set_xlim(-0.3, len(months) - 0.7)

fig.tight_layout()

output_path = os.path.join(os.path.dirname(__file__), "backtest_predicted_vs_actual.png")
fig.savefig(output_path, dpi=150, bbox_inches='tight')
print(f"\nPlot saved to: {output_path}")
plt.close()
