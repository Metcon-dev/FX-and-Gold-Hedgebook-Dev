"""
Backtest Plot: Gradient Boosting Model — Predicted vs Actual (Last 6 Months)
=============================================================================
Walk-forward backtest using the same GradientBoostingRegressor from
purchases_ml_service.py. For each test month, trains on ALL prior data,
then forecasts daily spend for the month using MC price/weight sampling.
"""

import requests as http_req
import math
import time
import os
import logging
import warnings
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import r2_score

warnings.filterwarnings("ignore")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("ERROR: pip install matplotlib")
    exit(1)

# ---------- Config ----------
TRADEMC_BASE = "https://trademc-admin.metcon.co.za"
TRADEMC_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"
GRAMS_PER_TROY_OUNCE = 31.1035
N_PATHS = 200
TEST_MONTHS = 6

# ---------- Fetch ----------
print("Fetching all TradeMC trades...")
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
    if len(all_trades) % 2000 < 100:
        print(f"  {len(all_trades)} trades...")
    if len(batch) < 100:
        break
print(f"  Done: {len(all_trades)} trades")

print("Fetching companies...")
r = http_req.get(f"{TRADEMC_BASE}/items/company", headers=headers, params={"limit": 500}, timeout=15)
companies = {}
if r.status_code == 200:
    for c in r.json().get("data", []):
        companies[c.get("id")] = c
print(f"  {len(companies)} companies")

# ---------- Helpers (same as purchases_ml_service.py) ----------
def _parse_num(v):
    if v is None or v == "":
        return None
    try:
        n = float(str(v).replace(",", "").strip())
        return n if math.isfinite(n) else None
    except Exception:
        return None

def _to_ts(v):
    if not v:
        return None
    s = str(v)
    for f in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.000Z", "%Y-%m-%dT%H:%M:%SZ",
              "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:min(len(s), 26)].rstrip("Z"), f.rstrip("Z"))
        except Exception:
            continue
    return None

def _clamp(x, lo, hi):
    return max(lo, min(hi, x))

# ---------- Build daily DataFrame (same logic as ML service) ----------
def build_daily_df(trades, companies):
    daily_agg = defaultdict(lambda: {
        "weight": 0, "payment": 0, "trade_count": 0,
        "pw": 0, "pwt": 0, "fw": 0, "fwt": 0, "rw": 0, "rwt": 0,
    })

    for row in trades:
        weight = _parse_num(row.get("weight"))
        if weight is None or weight <= 0:
            continue
        dt = _to_ts(row.get("trade_timestamp") or row.get("date_created"))
        if dt is None:
            continue

        cid = row.get("company_id") or row.get("company")
        co = companies.get(cid, {}) if cid else {}
        ref_rate = _parse_num(co.get("refining_rate")) or 0

        usd = _parse_num(row.get("usd_per_troy_ounce_confirmed") or row.get("usd_per_troy_ounce"))
        fx = _parse_num(row.get("zar_to_usd_confirmed") or row.get("zar_to_usd"))
        if usd is None:
            zar_oz = _parse_num(row.get("zar_per_troy_ounce_confirmed") or row.get("zar_per_troy_ounce"))
            if zar_oz and fx and abs(fx) > 1e-9:
                usd = zar_oz / fx
        if usd is None or fx is None:
            continue

        zar_per_gram = usd * fx / GRAMS_PER_TROY_OUNCE
        zar_less_ref = zar_per_gram * (1 - ref_rate / 100)
        payment = weight * zar_less_ref
        rf = _clamp(zar_less_ref / max(1e-9, zar_per_gram), 0.5, 1.2)

        dk = dt.strftime("%Y-%m-%d")
        d = daily_agg[dk]
        d["weight"] += weight
        d["payment"] += payment
        d["trade_count"] += 1
        d["pw"] += usd * weight
        d["pwt"] += weight
        d["fw"] += fx * weight
        d["fwt"] += weight
        d["rw"] += rf * weight
        d["rwt"] += weight

    rows = []
    for dk in sorted(daily_agg.keys()):
        d = daily_agg[dk]
        if d["weight"] <= 0 or d["pwt"] <= 0:
            continue
        rows.append({
            "date": dk, "weight_g": d["weight"], "payment_zar": d["payment"],
            "trade_count": d["trade_count"],
            "spot_usd": d["pw"] / d["pwt"],
            "fx_rate": d["fw"] / d["fwt"] if d["fwt"] > 0 else np.nan,
            "refine_factor": d["rw"] / d["rwt"] if d["rwt"] > 0 else 1,
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["daily_spend"] = df["payment_zar"]
    return df


def add_features(df):
    df = df.copy()
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_month"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["week_of_month"] = (df["day_of_month"] - 1) // 7 + 1
    df["is_month_start"] = (df["day_of_month"] <= 5).astype(int)
    df["is_month_end"] = (df["day_of_month"] >= 25).astype(int)
    df["days_in_month"] = df["date"].dt.days_in_month

    df["zar_per_gram"] = df["spot_usd"] * df["fx_rate"] / GRAMS_PER_TROY_OUNCE
    df["zar_per_gram_net"] = df["zar_per_gram"] * df["refine_factor"]

    for w in [5, 10, 21, 63]:
        df[f"spot_ma_{w}"] = df["spot_usd"].rolling(w, min_periods=1).mean()
        df[f"fx_ma_{w}"] = df["fx_rate"].rolling(w, min_periods=1).mean()
        if w >= 5:
            df[f"spot_vol_{w}"] = df["spot_usd"].pct_change().rolling(w, min_periods=2).std()
            df[f"fx_vol_{w}"] = df["fx_rate"].pct_change().rolling(w, min_periods=2).std()

    df["spot_ret_1d"] = df["spot_usd"].pct_change()
    df["spot_ret_5d"] = df["spot_usd"].pct_change(5)
    df["spot_ret_21d"] = df["spot_usd"].pct_change(21)
    df["fx_ret_1d"] = df["fx_rate"].pct_change()
    df["fx_ret_5d"] = df["fx_rate"].pct_change(5)
    df["fx_ret_21d"] = df["fx_rate"].pct_change(21)

    for w in [5, 10, 21]:
        df[f"weight_ma_{w}"] = df["weight_g"].shift(1).rolling(w, min_periods=1).mean()
        df[f"spend_ma_{w}"] = df["daily_spend"].shift(1).rolling(w, min_periods=1).mean()
        df[f"trades_ma_{w}"] = df["trade_count"].shift(1).rolling(w, min_periods=1).mean()

    for lag in [1, 2, 3, 5, 10, 21]:
        df[f"spend_lag_{lag}"] = df["daily_spend"].shift(lag)
        df[f"weight_lag_{lag}"] = df["weight_g"].shift(lag)

    df["year_month"] = df["date"].dt.to_period("M")
    monthly = df.groupby("year_month").agg(
        monthly_spend=("daily_spend", "sum"),
        monthly_weight=("weight_g", "sum"),
        monthly_trades=("trade_count", "sum"),
        monthly_days=("daily_spend", "count"),
    ).reset_index()
    monthly["prev_month_spend"] = monthly["monthly_spend"].shift(1)
    monthly["prev_month_weight"] = monthly["monthly_weight"].shift(1)
    monthly["prev_month_avg_daily"] = monthly["prev_month_spend"] / monthly["monthly_days"].shift(1).replace(0, np.nan)
    month_map = {str(row["year_month"]): row for _, row in monthly.iterrows()}

    df["prev_month_spend"] = df["year_month"].apply(
        lambda ym: month_map.get(str(ym - 1), {}).get("monthly_spend", np.nan)
    )
    df["prev_month_weight"] = df["year_month"].apply(
        lambda ym: month_map.get(str(ym - 1), {}).get("monthly_weight", np.nan)
    )
    df["prev_month_avg_daily"] = df["year_month"].apply(
        lambda ym: month_map.get(str(ym - 1), {}).get("prev_month_avg_daily", np.nan)
    )

    df["spot_x_fx"] = df["spot_usd"] * df["fx_rate"]
    df["spot_x_weight_lag1"] = df["spot_usd"] * df["weight_lag_1"].fillna(0)
    df = df.drop(columns=["year_month"])

    feature_cols = [c for c in df.columns if c not in ("date", "daily_spend", "payment_zar")]
    return df, feature_cols


# ---------- Build full daily df ----------
print("\nBuilding daily DataFrame...")
full_df = build_daily_df(all_trades, companies)
print(f"  {len(full_df)} trade days: {full_df['date'].min().date()} to {full_df['date'].max().date()}")

# Monthly actuals
full_df["ym"] = full_df["date"].dt.to_period("M")
month_actuals = full_df.groupby("ym")["daily_spend"].sum().to_dict()
all_months = sorted(month_actuals.keys())

now = datetime.now()
current_month = pd.Period(now.strftime("%Y-%m"), "M")
test_candidates = [m for m in all_months if m < current_month]
test_months = test_candidates[-TEST_MONTHS:]
print(f"  Backtesting {len(test_months)} months: {test_months[0]} to {test_months[-1]}")

# ---------- Rolling helpers for forecast ----------
def _rolling_mean(vals, w):
    sub = vals[-w:] if len(vals) >= w else vals
    return sum(sub) / max(1, len(sub))

def _rolling_std(vals, w):
    sub = vals[-w:] if len(vals) >= w else vals
    if len(sub) < 2:
        return 0.0
    m = sum(sub) / len(sub)
    return math.sqrt(sum((v - m) ** 2 for v in sub) / (len(sub) - 1))

def _pct_change(vals, periods):
    if len(vals) > periods and vals[-periods - 1] != 0:
        return (vals[-1] - vals[-periods - 1]) / abs(vals[-periods - 1])
    return 0.0


# ---------- Walk-forward backtest ----------
print("\nRunning walk-forward GB backtest...")
results = []

for test_period in test_months:
    t0 = time.time()
    test_month_str = str(test_period)
    actual = month_actuals[test_period]

    # Split: train on everything before this month
    cutoff = pd.Timestamp(f"{test_month_str}-01")
    train_df = full_df[full_df["date"] < cutoff].drop(columns=["ym"]).copy()

    if len(train_df) < 60:
        print(f"  {test_month_str}: SKIP - only {len(train_df)} training days")
        continue

    # Add features to training set
    train_feat, feature_cols = add_features(train_df)
    train_clean = train_feat.dropna(subset=feature_cols + ["daily_spend"]).copy()

    if len(train_clean) < 30:
        print(f"  {test_month_str}: SKIP - only {len(train_clean)} clean rows")
        continue

    X_train = train_clean[feature_cols].values
    y_train = train_clean["daily_spend"].values

    # Train GB model
    model = GradientBoostingRegressor(
        n_estimators=500, max_depth=5, learning_rate=0.05,
        subsample=0.8, random_state=42,
    )
    model.fit(X_train, y_train)

    # In-sample R2 on last 100 days
    if len(X_train) > 100:
        tail_r2 = r2_score(y_train[-100:], model.predict(X_train[-100:]))
    else:
        tail_r2 = r2_score(y_train, model.predict(X_train))

    # --- Forecast the test month using MC paths (same as purchases_ml_service) ---
    year = int(test_month_str[:4])
    month = int(test_month_str[5:7])
    next_month_start = datetime(year, month, 1)
    if month == 12:
        month_after = datetime(year + 1, 1, 1)
    else:
        month_after = datetime(year, month + 1, 1)
    days_in_month = (month_after - next_month_start).days

    recent = train_clean.tail(63).copy()
    last_row = train_clean.iloc[-1]

    base_spot = float(last_row["spot_usd"])
    base_fx = float(last_row["fx_rate"])
    base_refine = float(last_row["refine_factor"])

    # Day-of-month profiles
    dom_weights = {d: [] for d in range(1, 32)}
    dom_trades = {d: [] for d in range(1, 32)}
    for _, row in train_clean.iterrows():
        dom = row["date"].day
        dom_weights[dom].append(float(row["weight_g"]))
        dom_trades[dom].append(float(row["trade_count"]))

    global_weight_mean = float(train_clean["weight_g"].mean())
    global_weight_std = float(train_clean["weight_g"].std())
    global_trade_mean = float(train_clean["trade_count"].mean())
    global_trade_std = float(train_clean["trade_count"].std())

    dom_weight_stats = {}
    dom_trade_stats = {}
    for dom in range(1, 32):
        wvals = dom_weights[dom]
        tvals = dom_trades[dom]
        if len(wvals) >= 3:
            dom_weight_stats[dom] = (np.mean(wvals), np.std(wvals))
            dom_trade_stats[dom] = (np.mean(tvals), np.std(tvals))
        else:
            dom_weight_stats[dom] = (global_weight_mean, global_weight_std)
            dom_trade_stats[dom] = (global_trade_mean, global_trade_std)

    spot_rets = train_clean["spot_usd"].pct_change().dropna()
    spot_daily_vol = float(spot_rets.std()) if len(spot_rets) > 10 else 0.01
    fx_rets = train_clean["fx_rate"].pct_change().dropna()
    fx_daily_vol = float(fx_rets.std()) if len(fx_rets) > 10 else 0.005

    recent_spots = recent["spot_usd"].tolist()
    recent_fxs = recent["fx_rate"].tolist()
    recent_weights = recent["weight_g"].tolist()
    recent_spends = recent["daily_spend"].tolist()
    recent_trades = recent["trade_count"].tolist()

    prev_month_spend = float(train_clean.tail(21)["daily_spend"].sum())
    prev_month_weight = float(train_clean.tail(21)["weight_g"].sum())
    prev_month_avg = prev_month_spend / max(1, len(train_clean.tail(21)))

    rng = np.random.default_rng(42)

    all_path_preds = np.zeros((N_PATHS, days_in_month))

    for path_idx in range(N_PATHS):
        r_spots = list(recent_spots)
        r_fxs = list(recent_fxs)
        r_weights = list(recent_weights)
        r_spends = list(recent_spends)
        r_trades = list(recent_trades)

        for d in range(days_in_month):
            dt = next_month_start + timedelta(days=d)
            day_of_month = dt.day
            day_of_week = dt.weekday()

            spot_shock = rng.normal(0, spot_daily_vol)
            fx_shock = rng.normal(0, fx_daily_vol)
            cur_spot = r_spots[-1] * math.exp(spot_shock)
            cur_fx = r_fxs[-1] * math.exp(fx_shock)

            w_mean, w_std = dom_weight_stats.get(day_of_month, (global_weight_mean, global_weight_std))
            sampled_weight = max(100, rng.normal(w_mean, w_std * 0.7))

            t_mean, t_std = dom_trade_stats.get(day_of_month, (global_trade_mean, global_trade_std))
            sampled_trades = max(1, rng.normal(t_mean, t_std * 0.7))

            features = {
                "weight_g": sampled_weight,
                "trade_count": sampled_trades,
                "spot_usd": cur_spot,
                "fx_rate": cur_fx,
                "refine_factor": base_refine,
                "day_of_week": day_of_week,
                "day_of_month": day_of_month,
                "month": dt.month,
                "week_of_month": (day_of_month - 1) // 7 + 1,
                "is_month_start": 1 if day_of_month <= 5 else 0,
                "is_month_end": 1 if day_of_month >= 25 else 0,
                "days_in_month": days_in_month,
                "zar_per_gram": cur_spot * cur_fx / GRAMS_PER_TROY_OUNCE,
                "zar_per_gram_net": cur_spot * cur_fx / GRAMS_PER_TROY_OUNCE * base_refine,
            }

            for w in [5, 10, 21, 63]:
                features[f"spot_ma_{w}"] = _rolling_mean(r_spots, w)
                features[f"fx_ma_{w}"] = _rolling_mean(r_fxs, w)
                if w >= 5:
                    s_rets = [r_spots[i] / r_spots[i - 1] - 1
                              for i in range(max(1, len(r_spots) - w), len(r_spots))
                              if r_spots[i - 1] != 0]
                    features[f"spot_vol_{w}"] = _rolling_std(s_rets, w) if s_rets else 0
                    f_rets = [r_fxs[i] / r_fxs[i - 1] - 1
                              for i in range(max(1, len(r_fxs) - w), len(r_fxs))
                              if r_fxs[i - 1] != 0]
                    features[f"fx_vol_{w}"] = _rolling_std(f_rets, w) if f_rets else 0

            features["spot_ret_1d"] = _pct_change(r_spots, 1)
            features["spot_ret_5d"] = _pct_change(r_spots, 5)
            features["spot_ret_21d"] = _pct_change(r_spots, 21)
            features["fx_ret_1d"] = _pct_change(r_fxs, 1)
            features["fx_ret_5d"] = _pct_change(r_fxs, 5)
            features["fx_ret_21d"] = _pct_change(r_fxs, 21)

            for w in [5, 10, 21]:
                features[f"weight_ma_{w}"] = _rolling_mean(r_weights, w)
                features[f"spend_ma_{w}"] = _rolling_mean(r_spends, w)
                features[f"trades_ma_{w}"] = _rolling_mean(r_trades, w)

            for lag in [1, 2, 3, 5, 10, 21]:
                features[f"spend_lag_{lag}"] = r_spends[-lag] if len(r_spends) >= lag else 0
                features[f"weight_lag_{lag}"] = r_weights[-lag] if len(r_weights) >= lag else 0

            features["prev_month_spend"] = prev_month_spend
            features["prev_month_weight"] = prev_month_weight
            features["prev_month_avg_daily"] = prev_month_avg
            features["spot_x_fx"] = cur_spot * cur_fx
            features["spot_x_weight_lag1"] = cur_spot * (r_weights[-1] if r_weights else 0)

            feat_vec = np.array([[features.get(c, 0) for c in feature_cols]])
            feat_vec = np.nan_to_num(feat_vec, nan=0.0, posinf=0.0, neginf=0.0)

            pred = max(0, float(model.predict(feat_vec)[0]))
            all_path_preds[path_idx, d] = pred

            r_spots.append(cur_spot)
            r_fxs.append(cur_fx)
            r_spends.append(pred)
            r_weights.append(sampled_weight)
            r_trades.append(sampled_trades)

    # Aggregate
    path_totals = all_path_preds.sum(axis=1)
    p10 = float(np.percentile(path_totals, 10))
    p25 = float(np.percentile(path_totals, 25))
    p50 = float(np.median(path_totals))
    p75 = float(np.percentile(path_totals, 75))
    p90 = float(np.percentile(path_totals, 90))

    err_pct = (p50 - actual) / actual * 100 if actual > 0 else 0
    in_band = p10 <= actual <= p90
    elapsed = time.time() - t0

    print(f"  {test_month_str}: Actual R{actual:,.0f}  |  GB P50 R{p50:,.0f}  |  Err {err_pct:+.1f}%  |  "
          f"R2(train)={tail_r2:.3f}  |  P10-P90: {'YES' if in_band else 'NO'}  |  {elapsed:.1f}s")

    results.append({
        'month': test_month_str,
        'actual': actual,
        'p10': p10, 'p25': p25, 'p50': p50, 'p75': p75, 'p90': p90,
        'train_r2': tail_r2,
    })

if not results:
    print("No results!")
    exit(1)

# ---------- Compute out-of-sample R2 ----------
actuals_arr = np.array([r['actual'] for r in results])
preds_arr = np.array([r['p50'] for r in results])
oos_r2 = r2_score(actuals_arr, preds_arr)
mae_pct = np.mean(np.abs((preds_arr - actuals_arr) / actuals_arr)) * 100

print(f"\n  OUT-OF-SAMPLE METRICS ({len(results)} months):")
print(f"  Out-of-sample R2:          {oos_r2:.4f}")
print(f"  Mean Abs % Error (MAPE):   {mae_pct:.1f}%")
print(f"  Avg train R2 (in-sample):  {np.mean([r['train_r2'] for r in results]):.4f}")

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
ax.fill_between(x, p10s, p90s, alpha=0.12, color='#4CAF50', label='P10-P90 band')
ax.fill_between(x, p25s, p75s, alpha=0.25, color='#4CAF50', label='P25-P75 band')

# Lines
ax.plot(x, actuals, 'o-', color='#E53935', linewidth=2.5, markersize=11, label='Actual', zorder=5)
ax.plot(x, p50s, 's--', color='#1B5E20', linewidth=2.5, markersize=11, label='GB Predicted (P50)', zorder=5)

# Annotate values
for i in x:
    ax.annotate(f'R{actuals[i]/1e6:.1f}M', (i, actuals[i]),
                textcoords="offset points", xytext=(0, 14), ha='center',
                fontsize=9, color='#E53935', fontweight='bold')
    ax.annotate(f'R{p50s[i]/1e6:.1f}M', (i, p50s[i]),
                textcoords="offset points", xytext=(0, -18), ha='center',
                fontsize=9, color='#1B5E20', fontweight='bold')

# Error % labels
for i in x:
    err = (p50s[i] - actuals[i]) / actuals[i] * 100 if actuals[i] > 0 else 0
    mid = (actuals[i] + p50s[i]) / 2
    ax.annotate(f'{err:+.1f}%', (i, mid),
                textcoords="offset points", xytext=(32, 0), ha='left',
                fontsize=8, color='#555', fontstyle='italic',
                arrowprops=dict(arrowstyle='->', color='#aaa', lw=0.8))

# Metrics box
metrics_text = f"Out-of-sample R\u00b2: {oos_r2:.3f}\nMAPE: {mae_pct:.1f}%\nAvg train R\u00b2: {np.mean([r['train_r2'] for r in results]):.3f}"
ax.text(0.98, 0.02, metrics_text, transform=ax.transAxes, fontsize=10,
        verticalalignment='bottom', horizontalalignment='right',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='#E8F5E9', edgecolor='#4CAF50', alpha=0.9),
        fontfamily='monospace')

ax.set_xticks(list(x))
ax.set_xticklabels(months, fontsize=11, fontweight='bold')
ax.set_xlabel('Month', fontsize=12, fontweight='bold')
ax.set_ylabel('Monthly Purchase Amount (ZAR)', fontsize=12, fontweight='bold')
ax.set_title('GB Model Backtest: Predicted vs Actual Purchase Orders (Last 6 Months)',
             fontsize=14, fontweight='bold', pad=15)

ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'R{v/1e6:.0f}M' if v >= 1e6 else f'R{v/1e3:.0f}K'))
ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
ax.grid(True, alpha=0.3, linestyle='--')
ax.set_xlim(-0.3, len(months) - 0.7)

fig.tight_layout()

output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_gb_predicted_vs_actual.png")
fig.savefig(output_path, dpi=150, bbox_inches='tight')
print(f"\nPlot saved to: {output_path}")
plt.close()
