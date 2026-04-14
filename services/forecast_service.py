"""
Forecast Service — yfinance data + Geometric Brownian Motion Monte Carlo.

Fetches 5 years of daily data from yfinance, then runs Monte Carlo
simulations at 4-hour resolution to forecast 1 month ahead.

Data sources:
  - Gold (XAU/USD): yfinance GC=F
  - USD/ZAR: yfinance USDZAR=X
  - Purchases (XAU/ZAR): Derived from Gold x USD/ZAR

Key design choices:
  - No learned directional bias: drift (mu) is estimated purely from historical
    log returns, not from a trained model that overfits recent trends.
  - Geometric Brownian Motion ensures prices stay positive and returns are
    log-normally distributed — matching real FX / commodity price behaviour.
  - Volatility is estimated from historical data using an exponentially-weighted
    moving average (EWMA) with optional flat-weight fallback.
  - Confidence intervals come from the full distribution of 10,000 simulation
    paths rather than from parametric assumptions.
"""

import math
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    import yfinance as yf
    _HAS_YFINANCE = True
except ImportError:
    _HAS_YFINANCE = False

# In-memory cache: key -> (timestamp, data)
_price_cache: Dict[str, Tuple[float, Any]] = {}
_CACHE_TTL = 600  # 10 minutes


# ---------------------------------------------------------------------------
# yfinance data fetchers
# ---------------------------------------------------------------------------

_YF_TICKER_MAP = {
    ("XAU", "USD"): "GC=F",       # Gold futures (tracks spot closely)
    ("USD", "ZAR"): "USDZAR=X",   # FX pair
    ("XAU", "ZAR"): None,         # Derived — not a single ticker
}


def _yf_fetch_history(from_symbol: str, to_symbol: str) -> List[Dict[str, Any]]:
    """Fetch 5 years of daily data from yfinance."""
    if not _HAS_YFINANCE:
        raise ImportError("yfinance is not installed — run: pip install yfinance")

    ticker_str = _YF_TICKER_MAP.get((from_symbol, to_symbol))
    if ticker_str is None:
        raise ValueError(f"No yfinance ticker for {from_symbol}/{to_symbol}")

    print(f"[FORECAST] Fetching {ticker_str} history from yfinance for {from_symbol}/{to_symbol}")
    ticker = yf.Ticker(ticker_str)
    df = ticker.history(period="5y", interval="1d")

    if df is None or df.empty:
        raise ValueError(f"yfinance returned no data for {ticker_str}")

    rows: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        close = float(row.get("Close", 0))
        if close <= 0:
            continue
        date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        rows.append({
            "date": date_str,
            "open": float(row.get("Open", close)),
            "high": float(row.get("High", close)),
            "low": float(row.get("Low", close)),
            "close": close,
        })

    rows.sort(key=lambda r: r["date"])
    print(f"[FORECAST] yfinance returned {len(rows)} rows for {from_symbol}/{to_symbol}")
    return rows


def _yf_fetch_current_price(from_symbol: str, to_symbol: str) -> Dict[str, Any]:
    """Fetch the latest price from yfinance."""
    if not _HAS_YFINANCE:
        raise ImportError("yfinance is not installed — run: pip install yfinance")

    ticker_str = _YF_TICKER_MAP.get((from_symbol, to_symbol))
    if ticker_str is None:
        raise ValueError(f"No yfinance ticker for {from_symbol}/{to_symbol}")

    ticker = yf.Ticker(ticker_str)
    price = None

    # Try fast_info first
    try:
        price = ticker.fast_info.get("lastPrice", None)
        if price is None or price <= 0:
            price = ticker.fast_info.get("last_price", None)
    except Exception:
        pass

    # Fall back to recent history
    if price is None or price <= 0:
        df = ticker.history(period="5d", interval="1d")
        if df is not None and not df.empty:
            price = float(df["Close"].iloc[-1])

    if price is None or price <= 0:
        raise ValueError(f"yfinance returned no current price for {ticker_str}")

    return {
        "from": from_symbol,
        "to": to_symbol,
        "rate": float(price),
        "last_refreshed": datetime.utcnow().strftime("%Y-%m-%d"),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "yfinance",
    }


def _derive_xau_zar(gold_rows: List[Dict[str, Any]], fx_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Derive XAU/ZAR by multiplying gold (XAU/USD) x USD/ZAR on matching dates."""
    fx_by_date = {r["date"]: r["close"] for r in fx_rows}
    rows: List[Dict[str, Any]] = []
    for g in gold_rows:
        fx_rate = fx_by_date.get(g["date"])
        if fx_rate is None:
            continue
        zar_price = g["close"] * fx_rate
        rows.append({
            "date": g["date"],
            "open": zar_price,
            "high": zar_price,
            "low": zar_price,
            "close": round(zar_price, 2),
        })
    return rows


# ---------------------------------------------------------------------------
# Public data API
# ---------------------------------------------------------------------------

def fetch_daily_data(
    from_symbol: str,
    to_symbol: str,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch up to 5 years of daily data for any supported pair via yfinance.

      - XAU/USD -> yfinance GC=F
      - USD/ZAR -> yfinance USDZAR=X
      - XAU/ZAR -> derived from Gold x USD/ZAR
    """
    cache_key = f"daily_{from_symbol}_{to_symbol}"
    now = time.time()
    if cache_key in _price_cache:
        ts, data = _price_cache[cache_key]
        if now - ts < _CACHE_TTL:
            return data

    if from_symbol == "XAU" and to_symbol == "ZAR":
        gold = fetch_daily_data("XAU", "USD")
        fx = fetch_daily_data("USD", "ZAR")
        rows = _derive_xau_zar(gold, fx)
    else:
        rows = _yf_fetch_history(from_symbol, to_symbol)

    _price_cache[cache_key] = (now, rows)
    return rows


def fetch_current_price(
    from_symbol: str,
    to_symbol: str,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch the latest price via yfinance.

    - XAU/USD -> yfinance GC=F
    - USD/ZAR -> yfinance USDZAR=X
    - XAU/ZAR -> derived from gold spot x USD/ZAR
    """
    cache_key = f"current_{from_symbol}_{to_symbol}"
    now = time.time()
    if cache_key in _price_cache:
        ts, data = _price_cache[cache_key]
        if now - ts < 60:
            return data

    if from_symbol == "XAU" and to_symbol == "ZAR":
        gold = fetch_current_price("XAU", "USD")
        fx = fetch_current_price("USD", "ZAR")
        result = {
            "from": "XAU", "to": "ZAR",
            "rate": round(gold["rate"] * fx["rate"], 2),
            "last_refreshed": gold.get("last_refreshed", ""),
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "source": "yfinance",
        }
    else:
        result = _yf_fetch_current_price(from_symbol, to_symbol)

    _price_cache[cache_key] = (now, result)
    return result


# ---------------------------------------------------------------------------
# Monte Carlo simulation (Geometric Brownian Motion)
# ---------------------------------------------------------------------------

def _compute_log_returns(closes: np.ndarray) -> np.ndarray:
    return np.diff(np.log(closes))


def _estimate_drift_and_vol(
    log_returns: np.ndarray,
    ewma_halflife: Optional[int] = 60,
) -> Tuple[float, float]:
    """Estimate annualised drift (mu) and volatility (sigma)."""
    trading_days_per_year = 252

    mu_daily = float(np.mean(log_returns))
    mu_annual = mu_daily * trading_days_per_year

    if ewma_halflife and len(log_returns) > ewma_halflife:
        n = len(log_returns)
        lam = 1 - np.exp(-np.log(2) / ewma_halflife)
        weights = (1 - lam) ** np.arange(n - 1, -1, -1)
        weights /= weights.sum()
        weighted_mean = np.dot(weights, log_returns)
        variance = np.dot(weights, (log_returns - weighted_mean) ** 2)
        sigma_daily = float(np.sqrt(variance))
    else:
        sigma_daily = float(np.std(log_returns, ddof=1))

    sigma_annual = sigma_daily * math.sqrt(trading_days_per_year)
    return mu_annual, sigma_annual


def run_monte_carlo(
    daily_prices: List[Dict[str, Any]],
    forecast_days: int = 30,
    n_simulations: int = 10_000,
    steps_per_day: int = 6,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Run a GBM Monte Carlo simulation."""
    closes = np.array([d["close"] for d in daily_prices], dtype=np.float64)
    if len(closes) < 30:
        raise ValueError(f"Need at least 30 data points, got {len(closes)}")

    log_returns = _compute_log_returns(closes)
    mu, sigma = _estimate_drift_and_vol(log_returns)

    current_price = float(closes[-1])
    trading_days_per_year = 252

    dt = 1.0 / (trading_days_per_year * steps_per_day)
    total_steps = forecast_days * steps_per_day

    rng = np.random.default_rng(seed)
    Z = rng.standard_normal((n_simulations, total_steps))

    drift_term = (mu - 0.5 * sigma ** 2) * dt
    diffusion_term = sigma * math.sqrt(dt) * Z

    log_increments = drift_term + diffusion_term
    log_paths = np.cumsum(log_increments, axis=1)
    log_paths = np.concatenate([np.zeros((n_simulations, 1)), log_paths], axis=1)
    paths = current_price * np.exp(log_paths)

    percentiles = [5, 10, 25, 50, 75, 90, 95]
    pct_matrix = np.percentile(paths, percentiles, axis=0)
    mean_path = np.mean(paths, axis=0)

    daily_summary = []
    base_date = datetime.utcnow()
    for day in range(1, forecast_days + 1):
        step_idx = day * steps_per_day
        if step_idx > total_steps:
            break
        day_date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        entry = {
            "day": day,
            "date": day_date,
            "mean": round(float(mean_path[step_idx]), 4),
            "p5": round(float(pct_matrix[0, step_idx]), 4),
            "p10": round(float(pct_matrix[1, step_idx]), 4),
            "p25": round(float(pct_matrix[2, step_idx]), 4),
            "p50": round(float(pct_matrix[3, step_idx]), 4),
            "p75": round(float(pct_matrix[4, step_idx]), 4),
            "p90": round(float(pct_matrix[5, step_idx]), 4),
            "p95": round(float(pct_matrix[6, step_idx]), 4),
            "pct_change": round(
                (float(mean_path[step_idx]) / current_price - 1) * 100, 4
            ),
        }
        daily_summary.append(entry)

    final_prices = paths[:, -1]
    prob_up = round(float(np.mean(final_prices > current_price)) * 100, 2)

    hist_window = min(90, len(daily_prices))
    historical = [
        {"date": d["date"], "close": round(d["close"], 4)}
        for d in daily_prices[-hist_window:]
    ]

    return {
        "current_price": round(current_price, 4),
        "mu_annual": round(mu, 6),
        "sigma_annual": round(sigma, 6),
        "forecast_days": forecast_days,
        "steps_per_day": steps_per_day,
        "simulation_count": n_simulations,
        "prob_up": prob_up,
        "daily_summary": daily_summary,
        "historical": historical,
    }


# ---------------------------------------------------------------------------
# High-level API for the Flask routes
# ---------------------------------------------------------------------------

def get_forecast(
    from_symbol: str,
    to_symbol: str,
    forecast_days: int = 30,
    n_simulations: int = 10_000,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Full pipeline: fetch data -> run Monte Carlo -> return results."""
    daily = fetch_daily_data(from_symbol, to_symbol)
    if not daily:
        raise ValueError(f"No historical data for {from_symbol}/{to_symbol}")

    result = run_monte_carlo(
        daily,
        forecast_days=forecast_days,
        n_simulations=n_simulations,
    )
    result["pair"] = f"{from_symbol}/{to_symbol}"
    result["data_points"] = len(daily)
    result["data_start"] = daily[0]["date"]
    result["data_end"] = daily[-1]["date"]
    return result


def invalidate_cache(from_symbol: Optional[str] = None, to_symbol: Optional[str] = None):
    """Clear cached data so next request fetches fresh."""
    if from_symbol and to_symbol:
        _price_cache.pop(f"daily_{from_symbol}_{to_symbol}", None)
        _price_cache.pop(f"current_{from_symbol}_{to_symbol}", None)
    else:
        _price_cache.clear()
