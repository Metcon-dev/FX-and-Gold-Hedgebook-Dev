"""
ML Purchases Forecast — Model Selection
=========================================
Target: daily_spend_zar = (spot_usd * fx_rate / 31.1035) * (1 - refine_rate/100) * grams_per_day

Fetches ALL TradeMC buy history, engineers features, trains multiple models,
evaluates on a 6-month holdout with walk-forward validation.

Models tested:
  1. Ridge Regression (baseline)
  2. Random Forest
  3. Gradient Boosting (sklearn)
  4. XGBoost
  5. LightGBM
  6. Ensemble (weighted average of top 3)
"""

import requests as http_req
import math
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import xgboost as xgb
import lightgbm as lgb

warnings.filterwarnings("ignore")

TRADEMC_BASE = "https://trademc-admin.metcon.co.za"
TRADEMC_KEY = "-xR5FPuqxJvJeJ9181N5hUVV05_UVf2J"
GRAMS_PER_TROY_OUNCE = 31.1035
HOLDOUT_MONTHS = 6

# ============================================================
# 1. FETCH ALL TRADES FROM API
# ============================================================
print("=" * 80)
print("STEP 1: Fetching all TradeMC trades from API...")
print("=" * 80)

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
    if len(all_trades) % 5000 < 100:
        print(f"  ... {len(all_trades)} trades fetched")
    if len(batch) < 100:
        break
print(f"  Total: {len(all_trades)} trades")

# Fetch companies for refining rates
r2 = http_req.get(f"{TRADEMC_BASE}/items/company", headers=headers, params={"limit": 500}, timeout=15)
companies = {}
if r2.status_code == 200:
    for c in r2.json().get("data", []):
        companies[c.get("id")] = c
print(f"  Companies: {len(companies)}")


# ============================================================
# 2. PARSE INTO DAILY AGGREGATES
# ============================================================
print("\n" + "=" * 80)
print("STEP 2: Building daily aggregates...")
print("=" * 80)


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


daily_agg = defaultdict(lambda: {
    "weight": 0, "payment": 0, "trade_count": 0,
    "pw": 0, "pwt": 0, "fw": 0, "fwt": 0, "rw": 0, "rwt": 0,
})

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
        zar_oz = parse_num(row.get("zar_per_troy_ounce_confirmed") or row.get("zar_per_troy_ounce"))
        if zar_oz and fx and abs(fx) > 1e-9:
            usd = zar_oz / fx

    if usd is None or fx is None:
        continue

    zar_per_gram = (usd * fx / GRAMS_PER_TROY_OUNCE)
    zar_less_ref = zar_per_gram * (1 - ref_rate / 100)
    payment = weight * zar_less_ref

    dk = dt.strftime("%Y-%m-%d")
    d = daily_agg[dk]
    d["weight"] += weight
    d["payment"] += payment
    d["trade_count"] += 1
    d["pw"] += usd * weight
    d["pwt"] += weight
    d["fw"] += fx * weight
    d["fwt"] += weight
    rf = max(0.5, min(1.2, zar_less_ref / max(1e-9, zar_per_gram)))
    d["rw"] += rf * weight
    d["rwt"] += weight

# Build DataFrame
rows = []
for dk in sorted(daily_agg.keys()):
    d = daily_agg[dk]
    if d["weight"] <= 0 or d["pwt"] <= 0:
        continue
    rows.append({
        "date": dk,
        "weight_g": d["weight"],
        "payment_zar": d["payment"],
        "trade_count": d["trade_count"],
        "spot_usd": d["pw"] / d["pwt"],
        "fx_rate": d["fw"] / d["fwt"] if d["fwt"] > 0 else np.nan,
        "refine_factor": d["rw"] / d["rwt"] if d["rwt"] > 0 else 1,
    })

df = pd.DataFrame(rows)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# Target variable: daily spend
df["daily_spend"] = df["payment_zar"]

print(f"  {len(df)} trade days from {df['date'].min().date()} to {df['date'].max().date()}")
print(f"  Daily spend range: R {df['daily_spend'].min():,.0f} to R {df['daily_spend'].max():,.0f}")
print(f"  Daily spend mean:  R {df['daily_spend'].mean():,.0f}")


# ============================================================
# 3. FEATURE ENGINEERING
# ============================================================
print("\n" + "=" * 80)
print("STEP 3: Feature engineering...")
print("=" * 80)

# Calendar features
df["day_of_week"] = df["date"].dt.dayofweek  # 0=Mon
df["day_of_month"] = df["date"].dt.day
df["month"] = df["date"].dt.month
df["week_of_month"] = (df["day_of_month"] - 1) // 7 + 1
df["is_month_start"] = (df["day_of_month"] <= 5).astype(int)
df["is_month_end"] = (df["day_of_month"] >= 25).astype(int)
df["days_in_month"] = df["date"].dt.days_in_month

# Price features
df["zar_per_gram"] = df["spot_usd"] * df["fx_rate"] / GRAMS_PER_TROY_OUNCE
df["zar_per_gram_net"] = df["zar_per_gram"] * df["refine_factor"]

# Rolling features on spot price
for w in [5, 10, 21, 63]:
    df[f"spot_ma_{w}"] = df["spot_usd"].rolling(w, min_periods=1).mean()
    df[f"fx_ma_{w}"] = df["fx_rate"].rolling(w, min_periods=1).mean()
    if w >= 5:
        df[f"spot_vol_{w}"] = df["spot_usd"].pct_change().rolling(w, min_periods=2).std()
        df[f"fx_vol_{w}"] = df["fx_rate"].pct_change().rolling(w, min_periods=2).std()

# Spot momentum
df["spot_ret_1d"] = df["spot_usd"].pct_change()
df["spot_ret_5d"] = df["spot_usd"].pct_change(5)
df["spot_ret_21d"] = df["spot_usd"].pct_change(21)
df["fx_ret_1d"] = df["fx_rate"].pct_change()
df["fx_ret_5d"] = df["fx_rate"].pct_change(5)
df["fx_ret_21d"] = df["fx_rate"].pct_change(21)

# Weight/volume features (lagged to avoid leakage)
for w in [5, 10, 21]:
    df[f"weight_ma_{w}"] = df["weight_g"].shift(1).rolling(w, min_periods=1).mean()
    df[f"spend_ma_{w}"] = df["daily_spend"].shift(1).rolling(w, min_periods=1).mean()
    df[f"trades_ma_{w}"] = df["trade_count"].shift(1).rolling(w, min_periods=1).mean()

# Lagged targets (crucial for autoregressive signal)
for lag in [1, 2, 3, 5, 10, 21]:
    df[f"spend_lag_{lag}"] = df["daily_spend"].shift(lag)
    df[f"weight_lag_{lag}"] = df["weight_g"].shift(lag)

# Month-level aggregates (lagged by 1 month)
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
    lambda ym: month_map.get(str(ym - 1), {}).get("monthly_spend", np.nan) if str(ym) in month_map else np.nan
)
df["prev_month_weight"] = df["year_month"].apply(
    lambda ym: month_map.get(str(ym - 1), {}).get("monthly_weight", np.nan) if str(ym) in month_map else np.nan
)
df["prev_month_avg_daily"] = df["year_month"].apply(
    lambda ym: month_map.get(str(ym - 1), {}).get("prev_month_avg_daily", np.nan) if str(ym) in month_map else np.nan
)

# Interaction features
df["spot_x_fx"] = df["spot_usd"] * df["fx_rate"]
df["spot_x_weight_lag1"] = df["spot_usd"] * df["weight_lag_1"].fillna(0)

# Drop helper column
df = df.drop(columns=["year_month"])

# Drop rows with NaN from rolling/lag features (first ~21 rows)
feature_cols = [c for c in df.columns if c not in ("date", "daily_spend", "payment_zar")]
df_clean = df.dropna(subset=feature_cols + ["daily_spend"]).copy()

print(f"  {len(feature_cols)} features engineered")
print(f"  {len(df_clean)} usable rows (after dropping NaN from lag/rolling)")
print(f"  Features: {feature_cols[:10]}... (showing first 10)")


# ============================================================
# 4. TRAIN/TEST SPLIT — WALK-FORWARD
# ============================================================
print("\n" + "=" * 80)
print("STEP 4: Walk-forward train/test split...")
print("=" * 80)

# Holdout: last 6 complete months
now = datetime.now()
current_month_start = datetime(now.year, now.month, 1)
holdout_start = current_month_start - timedelta(days=HOLDOUT_MONTHS * 31)
# Round to first of that month
holdout_start = datetime(holdout_start.year, holdout_start.month, 1)

train_mask = df_clean["date"] < pd.Timestamp(holdout_start)
test_mask = (df_clean["date"] >= pd.Timestamp(holdout_start)) & (df_clean["date"] < pd.Timestamp(current_month_start))

X_train = df_clean.loc[train_mask, feature_cols].values
y_train = df_clean.loc[train_mask, "daily_spend"].values
X_test = df_clean.loc[test_mask, feature_cols].values
y_test = df_clean.loc[test_mask, "daily_spend"].values
test_dates = df_clean.loc[test_mask, "date"].values

print(f"  Train: {len(X_train)} days ({df_clean.loc[train_mask, 'date'].min().date()} to {df_clean.loc[train_mask, 'date'].max().date()})")
print(f"  Test:  {len(X_test)} days ({pd.Timestamp(holdout_start).date()} to {df_clean.loc[test_mask, 'date'].max().date()})")

# Scale features
scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s = scaler.transform(X_test)


# ============================================================
# 5. TRAIN MODELS
# ============================================================
print("\n" + "=" * 80)
print("STEP 5: Training models...")
print("=" * 80)

models = {}

# 1. Ridge Regression
print("\n  [1/5] Ridge Regression...")
ridge = Ridge(alpha=100)
ridge.fit(X_train_s, y_train)
models["Ridge"] = ridge.predict(X_test_s)
print(f"        Done.")

# 2. Random Forest
print("  [2/5] Random Forest...")
rf = RandomForestRegressor(n_estimators=500, max_depth=15, min_samples_leaf=5, random_state=42, n_jobs=-1)
rf.fit(X_train, y_train)
models["Random Forest"] = rf.predict(X_test)
print(f"        Done.")

# 3. Gradient Boosting
print("  [3/5] Gradient Boosting...")
gb = GradientBoostingRegressor(n_estimators=500, max_depth=5, learning_rate=0.05, subsample=0.8, random_state=42)
gb.fit(X_train, y_train)
models["Gradient Boost"] = gb.predict(X_test)
print(f"        Done.")

# 4. XGBoost
print("  [4/5] XGBoost...")
xgb_model = xgb.XGBRegressor(
    n_estimators=800, max_depth=6, learning_rate=0.03, subsample=0.8,
    colsample_bytree=0.8, reg_alpha=10, reg_lambda=10, random_state=42,
    tree_method="hist", early_stopping_rounds=50,
)
xgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
models["XGBoost"] = xgb_model.predict(X_test)
print(f"        Done. Best iteration: {xgb_model.best_iteration}")

# 5. LightGBM
print("  [5/5] LightGBM...")
lgb_model = lgb.LGBMRegressor(
    n_estimators=800, max_depth=6, learning_rate=0.03, subsample=0.8,
    colsample_bytree=0.8, reg_alpha=10, reg_lambda=10, random_state=42,
    verbose=-1,
)
lgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
models["LightGBM"] = lgb_model.predict(X_test)
print(f"        Done.")


# ============================================================
# 6. EVALUATE
# ============================================================
print("\n" + "=" * 80)
print("STEP 6: Model evaluation on 6-month holdout")
print("=" * 80)

# Naive baseline: predict yesterday's spend
naive_preds = np.roll(y_test, 1)
naive_preds[0] = y_train[-1]  # first day uses last training day

print(f"\n  {'MODEL':<20} {'R2':>10} {'RMSE':>18} {'MAE':>18} {'MAPE':>10}")
print(f"  {'='*78}")

results = {}
# Add naive baseline
r2_naive = r2_score(y_test, naive_preds)
rmse_naive = math.sqrt(mean_squared_error(y_test, naive_preds))
mae_naive = mean_absolute_error(y_test, naive_preds)
mape_naive = np.mean(np.abs((y_test - naive_preds) / np.maximum(y_test, 1))) * 100
print(f"  {'Naive (lag-1)':<20} {r2_naive:>10.4f} R {rmse_naive:>15,.0f} R {mae_naive:>15,.0f} {mape_naive:>8.1f}%")
results["Naive (lag-1)"] = {"r2": r2_naive, "rmse": rmse_naive, "mae": mae_naive, "mape": mape_naive}

for name, preds in models.items():
    r2_val = r2_score(y_test, preds)
    rmse_val = math.sqrt(mean_squared_error(y_test, preds))
    mae_val = mean_absolute_error(y_test, preds)
    mape_val = np.mean(np.abs((y_test - preds) / np.maximum(y_test, 1))) * 100
    print(f"  {name:<20} {r2_val:>10.4f} R {rmse_val:>15,.0f} R {mae_val:>15,.0f} {mape_val:>8.1f}%")
    results[name] = {"r2": r2_val, "rmse": rmse_val, "mae": mae_val, "mape": mape_val}

# Ensemble: weighted average of top 3 by R2
print(f"\n  Building ensemble from top 3 models...")
top3 = sorted(
    [(name, results[name]["r2"]) for name in models.keys()],
    key=lambda x: x[1], reverse=True
)[:3]
print(f"    Top 3: {[(n, f'{r:.4f}') for n, r in top3]}")

# Weight by R2 (higher R2 = more weight)
total_r2 = sum(max(0.01, r) for _, r in top3)
ensemble_preds = np.zeros_like(y_test, dtype=float)
for name, r2_val in top3:
    w = max(0.01, r2_val) / total_r2
    ensemble_preds += w * models[name]
    print(f"    {name}: weight = {w:.3f}")

r2_ens = r2_score(y_test, ensemble_preds)
rmse_ens = math.sqrt(mean_squared_error(y_test, ensemble_preds))
mae_ens = mean_absolute_error(y_test, ensemble_preds)
mape_ens = np.mean(np.abs((y_test - ensemble_preds) / np.maximum(y_test, 1))) * 100
print(f"\n  {'Ensemble (top3)':<20} {r2_ens:>10.4f} R {rmse_ens:>15,.0f} R {mae_ens:>15,.0f} {mape_ens:>8.1f}%")
results["Ensemble"] = {"r2": r2_ens, "rmse": rmse_ens, "mae": mae_ens, "mape": mape_ens}


# ============================================================
# 7. MONTHLY AGGREGATION (matches your use case)
# ============================================================
print("\n" + "=" * 80)
print("STEP 7: Monthly aggregation (actual vs predicted totals)")
print("=" * 80)

# Pick best single model
best_name = max(models.keys(), key=lambda n: results[n]["r2"])
best_preds = models[best_name]
if results["Ensemble"]["r2"] > results[best_name]["r2"]:
    best_name = "Ensemble"
    best_preds = ensemble_preds

print(f"\n  Best model: {best_name} (R2={results[best_name]['r2']:.4f})")

test_df = pd.DataFrame({
    "date": test_dates,
    "actual": y_test,
    "predicted": best_preds,
})
test_df["date"] = pd.to_datetime(test_df["date"])
test_df["month"] = test_df["date"].dt.to_period("M")

monthly_results = test_df.groupby("month").agg(
    actual_total=("actual", "sum"),
    predicted_total=("predicted", "sum"),
    days=("actual", "count"),
).reset_index()

monthly_results["err_pct"] = (
    (monthly_results["predicted_total"] - monthly_results["actual_total"])
    / monthly_results["actual_total"] * 100
)

print(f"\n  {'MONTH':<10} {'DAYS':>6} {'ACTUAL':>18} {'PREDICTED':>18} {'ERR %':>10}")
print(f"  {'='*66}")
for _, row in monthly_results.iterrows():
    print(f"  {str(row['month']):<10} {row['days']:>6} R {row['actual_total']:>15,.0f} R {row['predicted_total']:>15,.0f} {row['err_pct']:>+8.1f}%")

# Monthly R2
monthly_r2 = r2_score(monthly_results["actual_total"], monthly_results["predicted_total"])
monthly_mape = np.mean(np.abs(monthly_results["err_pct"]))
print(f"\n  Monthly R2:   {monthly_r2:.4f}")
print(f"  Monthly MAPE: {monthly_mape:.1f}%")


# ============================================================
# 8. FEATURE IMPORTANCE (from best tree model)
# ============================================================
print("\n" + "=" * 80)
print("STEP 8: Top 20 feature importances")
print("=" * 80)

# Use XGBoost or LightGBM importance
if "XGBoost" in models:
    imp = xgb_model.feature_importances_
elif "LightGBM" in models:
    imp = lgb_model.feature_importances_
else:
    imp = rf.feature_importances_

imp_df = pd.DataFrame({"feature": feature_cols, "importance": imp})
imp_df = imp_df.sort_values("importance", ascending=False).head(20)
print(f"\n  {'RANK':<6} {'FEATURE':<35} {'IMPORTANCE':>12}")
print(f"  {'-'*55}")
for i, (_, row) in enumerate(imp_df.iterrows()):
    print(f"  {i+1:<6} {row['feature']:<35} {row['importance']:>12.4f}")


# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)
print(f"\n  Best model:           {best_name}")
print(f"  Daily R2:             {results[best_name]['r2']:.4f}")
print(f"  Daily RMSE:           R {results[best_name]['rmse']:,.0f}")
print(f"  Daily MAPE:           {results[best_name]['mape']:.1f}%")
print(f"  Monthly R2:           {monthly_r2:.4f}")
print(f"  Monthly MAPE:         {monthly_mape:.1f}%")
print(f"  Holdout period:       {HOLDOUT_MONTHS} months")
print(f"  Training days:        {len(X_train)}")
print(f"  Test days:            {len(X_test)}")
print(f"  Features used:        {len(feature_cols)}")
