"""
Backtest: Improved Purchases Forecast Model
============================================
Improvements over v1:
  1. Recency-weighted price/FX baselines (EWM last 60 days) instead of flat median
  2. Last-3-month weight average instead of 12-month (captures volume trends)
  3. Structural month-to-month variance added to widen P10-P90 bands
  4. Refining factor uses recent weighted average, not full-history median
"""
import requests as http_req, math, random
from datetime import datetime, timedelta
from collections import defaultdict

TRADEMC_BASE = "https://trademc-admin.metcon.co.za"
TRADEMC_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"
GRAMS_PER_TROY_OUNCE = 31.1035
SIM_PATHS = 2000
TEST_MONTHS = 6

headers = {"Authorization": f"Bearer {TRADEMC_KEY}", "Content-Type": "application/json"}
all_trades = []
last_id = 0
while True:
    params = {"limit": 100, "offset": 0, "sort": "id", "filter[id][_gt]": last_id}
    r = http_req.get(f"{TRADEMC_BASE}/items/trade", headers=headers, params=params, timeout=45)
    if r.status_code != 200:
        break
    batch = r.json().get("data", [])
    if not batch:
        break
    all_trades.extend(batch)
    last_id = max(int(t["id"]) for t in batch if t.get("id"))
    if len(batch) < 100:
        break
print(f"Loaded {len(all_trades)} trades")

r2 = http_req.get(f"{TRADEMC_BASE}/items/company", headers=headers, params={"limit": 500}, timeout=15)
companies = {c.get("id"): c for c in r2.json().get("data", [])} if r2.status_code == 200 else {}


def parse_num(v):
    if v is None or v == "":
        return None
    try:
        n = float(str(v).replace(",", "").strip())
        return n if math.isfinite(n) else None
    except Exception:
        return None


def to_ts(v):
    if not v:
        return None
    s = str(v)
    for f in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[: min(len(s), 26)].rstrip("Z"), f.rstrip("Z"))
        except Exception:
            continue
    return None


def quantile(a, q):
    if not a:
        return 0
    s = sorted(a)
    p = (len(s) - 1) * q
    lo = int(p)
    hi = min(lo + 1, len(s) - 1)
    w = p - lo
    return s[lo] * (1 - w) + s[hi] * w


def mean(a):
    return sum(a) / len(a) if a else 0


def std(a):
    if len(a) < 2:
        return 0
    m = mean(a)
    return math.sqrt(sum((v - m) ** 2 for v in a) / (len(a) - 1))


def median(a):
    return quantile(a, 0.5)


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def ewm_mean(values, halflife):
    """Exponentially weighted mean with given halflife (in samples)."""
    if not values:
        return 0
    alpha = 1 - math.exp(-math.log(2) / max(1, halflife))
    w = 0.0
    s = 0.0
    for i in range(len(values) - 1, -1, -1):
        age = len(values) - 1 - i
        wi = (1 - alpha) ** age
        s += wi * values[i]
        w += wi
    return s / w if w > 0 else values[-1]


# Parse trades into daily buckets
daily_data = {}
for row in all_trades:
    weight = parse_num(row.get("weight"))
    if weight is None or weight <= 0:
        continue
    dt = to_ts(row.get("trade_timestamp") or row.get("date_created"))
    if dt is None:
        continue
    cid = row.get("company_id") or row.get("company")
    co = companies.get(cid, {}) if cid else {}
    ref_rate = parse_num(co.get("refining_rate")) or 0
    usd = parse_num(row.get("usd_per_troy_ounce_confirmed") or row.get("usd_per_troy_ounce"))
    fx = parse_num(row.get("zar_to_usd_confirmed") or row.get("zar_to_usd"))
    if usd is None:
        zar_oz = parse_num(
            row.get("zar_per_troy_ounce_confirmed") or row.get("zar_per_troy_ounce")
        )
        if zar_oz and fx and abs(fx) > 1e-9:
            usd = zar_oz / fx
    zar_per_gram = (usd * fx / GRAMS_PER_TROY_OUNCE) if (usd and fx) else None
    zar_less_ref = zar_per_gram * (1 - ref_rate / 100) if zar_per_gram else None
    if zar_less_ref is None:
        continue
    payment = weight * zar_less_ref
    rf = 1.0
    if usd and fx:
        b = (usd * fx) / GRAMS_PER_TROY_OUNCE
        if b > 1e-9:
            rf = clamp(zar_less_ref / b, 0.5, 1.2)
    dk = dt.strftime("%Y-%m-%d")
    d = daily_data.get(dk)
    if d is None:
        d = {
            "day": dt.day, "weight": 0, "payment": 0,
            "pw": 0, "pwt": 0, "fw": 0, "fwt": 0, "rw": 0, "rwt": 0,
        }
        daily_data[dk] = d
    d["weight"] += weight
    d["payment"] += payment
    if usd:
        d["pw"] += usd * weight
        d["pwt"] += weight
    if fx:
        d["fw"] += fx * weight
        d["fwt"] += weight
    d["rw"] += rf * weight
    d["rwt"] += weight

all_daily = []
for dk, d in sorted(daily_data.items()):
    all_daily.append({
        "date": dk, "day": d["day"], "weight": d["weight"], "payment": d["payment"],
        "price": d["pw"] / d["pwt"] if d["pwt"] > 0 else float("nan"),
        "fx": d["fw"] / d["fwt"] if d["fwt"] > 0 else float("nan"),
        "refine_factor": d["rw"] / d["rwt"] if d["rwt"] > 0 else 1,
    })

month_totals = defaultdict(lambda: {"weight": 0, "payment": 0, "days": 0})
for d in all_daily:
    mk = d["date"][:7]
    month_totals[mk]["weight"] += d["weight"]
    month_totals[mk]["payment"] += d["payment"]
    month_totals[mk]["days"] += 1

now = datetime.now()
cm = now.strftime("%Y-%m")
test_months = [m for m in sorted(month_totals.keys()) if m < cm][-TEST_MONTHS:]


def run_forecast(target, hist):
    yr = int(target[:4])
    mo = int(target[5:7])
    dim = (datetime(yr + (1 if mo == 12 else 0), (mo % 12) + 1, 1) - datetime(yr, mo, 1)).days
    if len(hist) < 10:
        return None

    prices = [d["price"] for d in hist if math.isfinite(d["price"]) and d["price"] > 0]
    fxv = [d["fx"] for d in hist if math.isfinite(d["fx"]) and d["fx"] > 0]
    rv = [d["refine_factor"] for d in hist if d["refine_factor"] > 0]

    # --- IMPROVEMENT 1: Recency-weighted baselines ---
    # EWM with 30-day halflife on last 90 days of data captures recent price level
    recent_prices = prices[-90:] if len(prices) > 90 else prices
    recent_fx = fxv[-90:] if len(fxv) > 90 else fxv
    recent_refine = rv[-60:] if len(rv) > 60 else rv

    base_price = ewm_mean(recent_prices, halflife=30) if recent_prices else 3000
    base_fx = ewm_mean(recent_fx, halflife=30) if recent_fx else 18
    base_refine = ewm_mean(recent_refine, halflife=30) if recent_refine else 1

    def get_base_price(_):
        return base_price

    # --- IMPROVEMENT 2: Last 3 months weight average ---
    mm = defaultdict(float)
    for d in hist:
        mm[d["date"][:7]] += d["weight"]
    mw = list(mm.values())
    # Use last 3 months (more responsive) with fallback
    recent_months = mw[-3:] if len(mw) >= 3 else mw
    mwt = mean(recent_months) if recent_months else 1000

    # Day-of-month profile
    dp = [{"w": 0, "c": 0} for _ in range(31)]
    for d in hist:
        dp[d["day"] - 1]["w"] += d["weight"]
        dp[d["day"] - 1]["c"] += 1
    pr = [(dp[i]["w"] / dp[i]["c"] if dp[i]["c"] > 0 else 0) for i in range(dim)]
    ps = list(pr)
    for i, v in enumerate(pr):
        if v <= 0:
            pv = pr[i - 1] if i > 0 else 0
            nx = pr[i + 1] if i < len(pr) - 1 else 0
            ps[i] = (pv + nx) / 2
    sb = sum(ps)
    psh = [v / sb for v in ps] if sb > 1e-9 else [1 / dim] * dim

    # Price-weight correlation
    prets = []
    wrets = []
    for i in range(1, len(hist)):
        p0 = max(1e-6, hist[i - 1]["price"])
        p1 = max(1e-6, hist[i]["price"])
        if math.isfinite(p0) and math.isfinite(p1):
            prets.append(math.log(p1 / p0))
            wrets.append(math.log(max(1, hist[i]["weight"]) / max(1, hist[i - 1]["weight"])))

    pmn = mean(prets) if prets else 0
    wmn = mean(wrets) if wrets else 0
    cv = pv2 = 0
    for i in range(len(prets)):
        cv += (prets[i] - pmn) * (wrets[i] - wmn)
        pv2 += (prets[i] - pmn) ** 2
    beta = clamp(cv / pv2, -2.5, 2.5) if pv2 > 1e-12 else -0.2
    sp = clamp(std(prets), 0.0025, 0.08)
    res = [wrets[i] - beta * prets[i] for i in range(len(prets))]
    sw = clamp(std(res), 0.01, 0.35)

    # --- IMPROVEMENT 3: Structural month-to-month variance ---
    # Compute historical monthly payment variance to scale bands
    monthly_payments = [month_totals[mk]["payment"] for mk in sorted(mm.keys()) if mk in month_totals]
    if len(monthly_payments) >= 6:
        monthly_log_returns = []
        for i in range(1, len(monthly_payments)):
            if monthly_payments[i - 1] > 0 and monthly_payments[i] > 0:
                monthly_log_returns.append(math.log(monthly_payments[i] / monthly_payments[i - 1]))
        structural_vol = std(monthly_log_returns) if len(monthly_log_returns) >= 3 else 0.15
    else:
        structural_vol = 0.15
    # Clamp structural vol to reasonable range
    structural_vol = clamp(structural_vol, 0.05, 0.40)

    totals = []
    for _ in range(SIM_PATHS):
        # Apply structural shock once per month (scales entire month up/down)
        month_shock = math.exp(random.gauss(0, 1) * structural_vol)
        tot = 0
        pp = max(1, get_base_price(0))
        for dd in range(dim):
            bp = max(1, get_base_price(dd))
            sh = random.gauss(0, 1) * sp
            stp = max(1, pp * math.exp(sh))
            price = 0.65 * stp + 0.35 * bp
            rel = price / max(1e-6, get_base_price(dd))
            bdw = mwt * psh[dd]
            w = max(0, bdw * math.exp(beta * math.log(max(rel, 1e-6)) + random.gauss(0, 1) * sw))
            zpg = (price * base_fx / GRAMS_PER_TROY_OUNCE) * base_refine
            tot += max(0, w * zpg)
            pp = price
        totals.append(tot * month_shock)

    return {"p10": quantile(totals, 0.10), "p50": quantile(totals, 0.50), "p90": quantile(totals, 0.90)}


# Run backtest
actuals = []
preds = []
ib = 0

print(f"\nBacktesting {len(test_months)} months: {test_months[0]} to {test_months[-1]}")
print()
print(f"{'MONTH':<10} {'ACTUAL':>18} {'PRED P50':>18} {'ERR %':>10} {'IN BAND':>10}")
print("=" * 70)

for tm in test_months:
    actual = month_totals[tm]["payment"]
    ts = datetime.strptime(tm + "-01", "%Y-%m-%d")
    ws = ts - timedelta(days=35 * 30)
    hist = [d for d in all_daily if d["date"] < tm + "-01" and d["date"] >= ws.strftime("%Y-%m-%d")]
    if len(hist) < 10:
        print(f"{tm:<10} SKIP")
        continue
    random.seed(hash(tm) & 0xFFFFFFFF)
    result = run_forecast(tm, hist)
    if result is None:
        print(f"{tm:<10} SKIP")
        continue
    actuals.append(actual)
    preds.append(result["p50"])
    err = (result["p50"] - actual) / actual * 100 if actual > 0 else 0
    band = result["p10"] <= actual <= result["p90"]
    if band:
        ib += 1
    print(
        f"{tm:<10} R {actual:>15,.0f} R {result['p50']:>15,.0f} {err:>+8.1f}% {'YES' if band else 'NO':>10}"
    )

print("=" * 70)

if len(actuals) >= 2:
    ss_res = sum((a - p) ** 2 for a, p in zip(actuals, preds))
    a_mean = mean(actuals)
    ss_tot = sum((a - a_mean) ** 2 for a in actuals)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    mae = mean([abs(a - p) for a, p in zip(actuals, preds)])
    mape = mean([abs(a - p) / a * 100 for a, p in zip(actuals, preds) if a > 0])
    rmse = math.sqrt(mean([(a - p) ** 2 for a, p in zip(actuals, preds)]))
    coverage = ib / len(actuals) * 100

    print()
    print(f"  R-squared (R2):          {r_squared:.4f}")
    print(f"  RMSE:                    R {rmse:,.0f}")
    print(f"  MAE:                     R {mae:,.0f}")
    print(f"  MAPE:                    {mape:.1f}%")
    print(f"  P10-P90 Coverage:        {ib}/{len(actuals)} = {coverage:.0f}%")
