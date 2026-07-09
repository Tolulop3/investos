"""
InvestOS — ML Engine
=====================
XGBoost stock outperformance predictor + walk-forward backtester
Market regime filter + volatility-adjusted sizing + drawdown protection

Predicts: probability a stock outperforms S&P 500 over next 3 months
Trains on: rolling 5-year window, retrained monthly
Validates: strict walk-forward, no lookahead bias

v2.1 changes:
  - Half-Kelly position sizing calibrated to actual 1,466+ pick win rates
  - Score smoothing: 3-day EMA on ML probability to dampen single-day spikes
  - Kelly fractions: 60-74 tier is the real edge, 90-100 has negative Kelly

INSTALL: pip install xgboost scikit-learn pandas numpy yfinance --break-system-packages
"""

import json
import os
import time
import warnings
import urllib.request
import urllib.parse
import joblib
from datetime import datetime, timedelta
from collections import defaultdict

from gate_engine import MLGate, load_outcomes_ticker_counts, PROBATION_CAP

warnings.filterwarnings('ignore')

try:
    import numpy as np
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠️  pandas/numpy not installed.")

try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("⚠️  xgboost not installed.")

try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import roc_auc_score
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

# ============================================================
# CONFIG
# ============================================================

# ── Sector / regime encoding — MUST match ml_retrainer.py exactly ─────────────
_SECTOR_NORM_INF = {
    "financial services": "FINANCIALS", "financials": "FINANCIALS",
    "banks": "FINANCIALS", "insurance": "FINANCIALS",
    "asset management": "FINANCIALS", "capital markets": "FINANCIALS",
    "diversified financials": "FINANCIALS",
    "real estate": "REIT", "reits": "REIT", "reit": "REIT",
    "real estate investment trusts": "REIT",
    "energy": "ENERGY", "oil & gas": "ENERGY", "oil and gas": "ENERGY",
    "utilities": "UTILITIES",
    "consumer discretionary": "CONSUMER", "consumer staples": "CONSUMER",
    "technology": "TECH", "information technology": "TECH",
    "communication services": "TELECOM", "telecommunications": "TELECOM",
    "telecom": "TELECOM",
    "health care": "HEALTHCARE", "healthcare": "HEALTHCARE",
    "pharmaceuticals": "HEALTHCARE", "biotechnology": "HEALTHCARE",
    "industrials": "INDUSTRIALS",
    "materials": "MATERIALS",
    "pipelines": "PIPELINES",
}
_SECTOR_ENC_INF = {
    "FINANCIALS": 0, "REIT": 1, "ENERGY": 2, "UTILITIES": 3,
    "CONSUMER": 4, "TECH": 5, "TELECOM": 6, "HEALTHCARE": 7,
    "INDUSTRIALS": 8, "MATERIALS": 9, "PIPELINES": 10, "UNKNOWN": -1,
}

# Per-ticker sector override — used when yfinance returns no sector string.
# Values are in yfinance format (Title Case) so they pass through _SECTOR_NORM_INF.
# Add any ticker that shows UNKNOWN in the ML gate output here.
_TICKER_SECTOR_OVERRIDE = {
    "CAT":   "Industrials",      # Caterpillar — yfinance sometimes omits sector
    "ETN":   "Industrials",      # Eaton Corporation
    "PWR":   "Industrials",      # Quanta Services
    "POWL":  "Industrials",      # Powell Industries
    "MMM":   "Industrials",      # 3M Company
    "WEGZY": "Industrials",      # Westinghouse Electric (OTC)
    "MU":    "Technology",       # Micron Technology
    "AMZN":  "Consumer Discretionary",
    "BKNG":  "Consumer Discretionary",
    "CLF":   "Materials",        # Cleveland-Cliffs
    "PRYMF": "Materials",        # Pryme Petroleum (OTC)
    "BNY":   "Financial Services",  # Bank of New York Mellon
    "UMBF":  "Financial Services",  # UMB Financial
    "MS":    "Financial Services",  # Morgan Stanley
    "BMO.TO": "Financial Services", # Bank of Montreal
    "POW.TO": "Financial Services", # Power Corporation of Canada
    "BB.TO": "Technology",          # BlackBerry
    "FM.TO": "Materials",           # First Quantum Minerals
}


def _encode_sector(raw_sector_string, ticker=None):
    raw = raw_sector_string or ""
    if not raw.strip() and ticker:
        raw = _TICKER_SECTOR_OVERRIDE.get(ticker, "")
    key = _SECTOR_NORM_INF.get(raw.strip().lower(), "UNKNOWN")
    return _SECTOR_ENC_INF.get(key, -1)


ML_CONFIG = {
    "features": [
        "momentum_6m",
        "momentum_12m",
        "vol_adj_momentum",
        "roe",
        "profit_margin",
        "earnings_yield",
        "fcf_yield",
        "volatility_90d",
        "beta",
        "rev_growth",
        "earn_growth",
        "div_yield",
        "debt_equity",
        "rs_rating",
        "sector_momentum",
        "market_regime",
        "spx_vs_ma200",
        "news_boost",
        "close_to_ema20_ratio",
        "unified_regime_enc",
        "macro_regime_enc",
        "market_breadth_50ma",
        "sector_encoded",
    ],
    "xgb_params": {
        "n_estimators":     150,   # aligned with ml_retrainer.py
        "max_depth":        3,
        "learning_rate":    0.04,  # aligned with ml_retrainer.py
        "subsample":        0.8,
        "colsample_bytree": 0.7,
        "min_child_weight": 4,     # aligned with ml_retrainer.py
        "reg_alpha":        0.15,  # aligned with ml_retrainer.py
        "reg_lambda":       1.0,
        "random_state":     42,
        "eval_metric":      "auc",
        "verbosity":        0,
        "enable_categorical": True,   # required: sector_encoded uses 'category' dtype
        "monotone_constraints": {     # must match ml_retrainer.py exactly
            "roe":                  1,
            "profit_margin":        1,
            "earnings_yield":       1,
            "volatility_90d":      -1,
            "close_to_ema20_ratio":-1,
        },
    },
    "max_positions":        20,
    "min_positions":        10,
    "max_position_pct":     0.05,
    "max_sector_pct":       0.25,
    "training_window_years": 5,
    "prediction_horizon_months": 3,
    "max_portfolio_volatility": 0.15,
    "drawdown_reduction_trigger": 0.15,
    "drawdown_reduction_amount":  0.30,
    "regime_cash_pct":            0.50,
    "transaction_cost_bps":       15,
}

# ── Score smoothing cache (in-memory, persisted to JSON) ──────────────────────
# Stores last 3 ML probabilities per ticker for EMA smoothing.
# Prevents a single outlier day (99 → 72 in one run) from driving decisions.
_SMOOTH_CACHE_FILE = "ml_score_smooth.json"
_smooth_cache = {}

def _load_smooth_cache():
    global _smooth_cache
    try:
        if os.path.exists(_SMOOTH_CACHE_FILE):
            _smooth_cache = json.load(open(_SMOOTH_CACHE_FILE))
    except Exception:
        _smooth_cache = {}

def _save_smooth_cache():
    try:
        json.dump(_smooth_cache, open(_SMOOTH_CACHE_FILE, "w"), indent=2)
    except Exception:
        pass

def smooth_ml_prob(ticker, raw_prob, alpha=0.4):
    """
    3-day exponential moving average of ML probability.
    alpha=0.4: today's score weighted 40%, history 60%.
    Dampens spikes without lagging too far behind real moves.
    """
    global _smooth_cache
    history = _smooth_cache.get(ticker, [])
    if history:
        prev_ema = history[-1]
        smoothed = alpha * raw_prob + (1 - alpha) * prev_ema
    else:
        smoothed = raw_prob  # first run — no history
    # Keep last 3 values
    history = (history + [round(smoothed, 4)])[-3:]
    _smooth_cache[ticker] = history
    return round(smoothed, 4)


# ============================================================
# MARKET REGIME FILTER
# ============================================================

def get_market_regime(verbose=True):
    try:
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
               "^GSPC?interval=1d&range=1y")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())

        closes = [c for c in data['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
        if len(closes) < 200:
            return {"regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
                    "spx_price": 0, "ma200": 0, "pct_above_ma": 0}

        spx   = closes[-1]
        ma200 = sum(closes[-200:]) / 200
        ma50  = sum(closes[-50:])  / 50

        # Secondary intraday fetch: try 5m interval for a live SPX price
        # More current than EOD close (which yfinance caches until next session)
        try:
            _url2 = ("https://query1.finance.yahoo.com/v8/finance/chart/"
                     "^GSPC?interval=5m&range=1d")
            _req2 = urllib.request.Request(_url2, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(_req2, timeout=8) as _r2:
                _d2 = json.loads(_r2.read().decode())
            _c2 = [c for c in _d2['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if _c2:
                _live = _c2[-1]
                # Only use if it differs meaningfully from the stale close (>0.05%)
                if abs(_live - spx) / spx > 0.0005:
                    spx = _live
        except Exception:
            pass  # Fall back to daily close

        pct_diff = (spx - ma200) / ma200 * 100

        # Stale data guard: SPX high-watermark approach
        # The watermark only moves UP — stale cached data (which is LOWER) gets overridden
        try:
            import os as _os, json as _j
            _wmf = "spx_watermark.json"
            _watermark = 0
            if _os.path.exists(_wmf):
                try:
                    _watermark = _j.load(open(_wmf)).get("spx_high", 0)
                except Exception:
                    _watermark = 0
            # Always use the HIGHER of: current data vs stored watermark
            if _watermark > 0 and spx < _watermark * 0.970:  # 3% drop threshold — real moves are < 2% overnight
                print(f"  ⚠️  SPX stale data: {spx:.2f} < watermark {_watermark:.2f} × 0.970")
                # The watermark IS the last verified good price — use it directly
                # max(closes[-5:]) won't help if yfinance returned all stale values
                print(f"      Using watermark: {_watermark:.2f}")
                spx = _watermark
                pct_diff = (spx - ma200) / ma200 * 100
            # Update watermark if current SPX is higher
            if spx > _watermark:
                try:
                    _j.dump({"spx_high": round(spx, 2)}, open(_wmf, "w"))
                except Exception:
                    pass
        except Exception:
            pass

        if spx > ma200 and ma50 > ma200:
            regime = "BULL"; signal = "FULL_EXPOSURE"; cash_pct = 0.0
        elif spx > ma200 and ma50 <= ma200:
            regime = "RECOVERY"; signal = "CAUTIOUS_EXPOSURE"; cash_pct = 0.20
        elif spx <= ma200 and pct_diff > -5:
            regime = "CAUTION"; signal = "REDUCED_EXPOSURE"; cash_pct = 0.30
        else:
            regime = "BEAR"; signal = "DEFENSIVE"; cash_pct = ML_CONFIG["regime_cash_pct"]

        result = {
            "regime": regime, "signal": signal, "cash_pct": cash_pct,
            "spx_price": round(spx, 2), "ma200": round(ma200, 2),
            "ma50": round(ma50, 2), "pct_above_ma": round(pct_diff, 2),
            "full_exposure_pct": round((1 - cash_pct) * 100, 0)
        }

        if verbose:
            icon = "🟢" if regime == "BULL" else "🟡" if regime in ("RECOVERY","CAUTION") else "🔴"
            print(f"\n📊 MARKET REGIME: {icon} {regime}")
            print(f"   S&P 500: ${spx:,.2f} | 200-day MA: ${ma200:,.2f} | {pct_diff:+.1f}% above/below")
            print(f"   Signal: {signal} | Cash allocation: {cash_pct*100:.0f}%")

        return result
    except Exception as e:
        print(f"   ⚠️ Regime check failed: {e}")
        return {"regime": "UNKNOWN", "signal": "NEUTRAL", "cash_pct": 0.0,
                "spx_price": 0, "ma200": 0, "pct_above_ma": 0}


# ============================================================
# FEATURE BUILDER
# ============================================================

def build_features_for_stock(ticker, stock_data, rs_rating=50):
    if not HAS_PANDAS:
        return None
    try:
        try:
            url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
                   f"{urllib.parse.quote(ticker)}?interval=1mo&range=18mo")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read().decode())
            closes = [c for c in data['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
        except Exception:
            closes = []

        if len(closes) >= 13:
            mom_6m  = (closes[-2] - closes[-8])  / closes[-8]  if len(closes) >= 8  else 0
            mom_12m = (closes[-2] - closes[-14]) / closes[-14] if len(closes) >= 14 else 0
            daily_rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, min(91, len(closes)))]
            vol_90d = (sum(r**2 for r in daily_rets) / len(daily_rets)) ** 0.5 * (252 ** 0.5) if daily_rets else 0.2
        else:
            mom_6m  = stock_data.get("perf_90d", 0) / 100
            mom_12m = stock_data.get("perf_90d", 0) / 100 * 1.5
            vol_90d = stock_data.get("volatility", 2.0) / 100

        # EMA20 ratio — overextension signal: >1.05 = overbought, <0.95 = oversold
        try:
            ema_url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
                       f"{urllib.parse.quote(ticker)}?interval=1d&range=3mo")
            ema_req = urllib.request.Request(ema_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(ema_req, timeout=6) as er:
                ema_d = json.loads(er.read().decode())
            d_cls = [c for c in ema_d['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
            if len(d_cls) >= 20:
                alpha_e = 2.0 / 21.0
                ema_20d = d_cls[0]
                for c in d_cls[1:]:
                    ema_20d = alpha_e * c + (1.0 - alpha_e) * ema_20d
                close_to_ema20_ratio = round(d_cls[-1] / ema_20d if ema_20d > 0 else 1.0, 4)
            else:
                close_to_ema20_ratio = 1.0
        except Exception:
            close_to_ema20_ratio = 1.0

        roe           = stock_data.get("roe", 0) / 100
        profit_margin = stock_data.get("profit_margin", 0) / 100
        pe            = stock_data.get("pe_ratio", 20) or 20
        earnings_yield = 1 / pe if pe and pe > 0 else 0
        div_yield     = stock_data.get("div_yield", 0) / 100
        rev_growth    = stock_data.get("rev_growth", 0) / 100
        earn_growth   = stock_data.get("earn_growth", 0) / 100
        debt_equity   = min(stock_data.get("debt_equity", 1) or 1, 10) / 10
        fcf_yield     = max(0, profit_margin * 0.8)
        beta          = min(vol_90d / 0.15, 3.0)
        rs_norm       = rs_rating / 100

        raw_sector = stock_data.get("sector", "") or ""
        sector_enc = _encode_sector(raw_sector)

        return {
            "ticker": ticker,
            "momentum_6m":    round(mom_6m, 4),
            "momentum_12m":   round(mom_12m, 4),
            "roe":            round(roe, 4),
            "profit_margin":  round(profit_margin, 4),
            "earnings_yield": round(earnings_yield, 4),
            "fcf_yield":      round(fcf_yield, 4),
            "volatility_90d": round(vol_90d, 4),
            "beta":           round(beta, 4),
            "rev_growth":     round(rev_growth, 4),
            "earn_growth":    round(earn_growth, 4),
            "div_yield":      round(div_yield, 4),
            "debt_equity":    round(debt_equity, 4),
            "rs_rating":           round(rs_norm, 4),
            "market_regime":       0,
            "sector_momentum":     0,
            "spx_vs_ma200":        0.0,
            "news_boost":          0.0,
            "close_to_ema20_ratio": close_to_ema20_ratio,
            "unified_regime_enc":  2.0,  # default NEUTRAL; overwritten by run_daily.py
            "macro_regime_enc":    2.0,  # default NORMAL
            "market_breadth_50ma": 0.5,  # default unknown
            "sector_encoded":      sector_enc,
        }
    except Exception:
        return None


# ============================================================
# ML PREDICTOR
# ============================================================

class StockMLPredictor:
    def __init__(self):
        self.model              = None
        self.scaler             = None
        self.calibrator         = None
        self.trained            = False
        self.feature_importance = {}
        self.model_file         = "ml_model_state.json"

    def load_training_data(self):
        if not HAS_PANDAS:
            return None, None, None

        outcomes_file = "pick_outcomes.json"
        if os.path.exists(outcomes_file):
            try:
                raw = json.load(open(outcomes_file))
                resolved = [o for o in raw if o.get("outcome") and o.get("actual_return") is not None]
                if len(resolved) >= 100:
                    rows_X, rows_y, weights = [], [], []
                    now_ts = datetime.now().timestamp()
                    resolved_sorted = sorted(resolved, key=lambda o: o.get("signal_date", "2020-01-01"))
                    returns = [o.get("actual_return", 0) or 0 for o in resolved_sorted]
                    import numpy as _np2
                    global_med = float(_np2.median(returns))

                    for o in resolved_sorted:
                        mom_6m = o.get("perf_90d", 0) / 100
                        vol    = max(o.get("volatility", 2), 0.5) / 100
                        vol_adj_mom = max(min(mom_6m / max(vol, 0.01), 5.0), -5.0)
                        feat = {
                            "momentum_6m":    mom_6m,
                            "momentum_12m":   o.get("perf_90d", 0) / 100 * 1.4,
                            "vol_adj_momentum": vol_adj_mom,
                            "roe":            min(o.get("roe", 0) or 0, 100) / 100,
                            "profit_margin":  min(o.get("profit_margin", 0) or 0, 100) / 100,
                            "earnings_yield": 1/max(o.get("pe_ratio", 20) or 20, 1),
                            "fcf_yield":      max(o.get("profit_margin", 0) or 0, 0) / 100 * 0.8,
                            "volatility_90d": vol,
                            "beta":           min(vol / 0.15, 3.0),
                            "rev_growth":     o.get("rev_growth", 0) / 100,
                            "earn_growth":    o.get("earn_growth", 0) / 100,
                            "div_yield":      o.get("div_yield", 0) / 100,
                            "debt_equity":    min(o.get("debt_equity", 1) or 1, 10) / 10,
                            "rs_rating":      o.get("rs_rating", 50) / 100,
                            "sector_momentum":0.0,
                        }
                        rows_X.append(feat)
                        actual_ret = o.get("actual_return", 0) or 0
                        rows_y.append(1 if actual_ret > global_med else 0)
                        try:
                            sig_ts = datetime.strptime(o.get("signal_date","2020-01-01"),"%Y-%m-%d").timestamp()
                            days_old = max((now_ts - sig_ts) / 86400, 0)
                            w = float(_np2.exp(-days_old / 180.0))
                        except Exception:
                            w = 0.5
                        weights.append(max(w, 0.1))

                    X = pd.DataFrame(rows_X)[ML_CONFIG["features"]]
                    y = pd.Series(rows_y, dtype=int)
                    w_arr = _np2.array(weights)
                    win_rate = float(y.mean())
                    print(f"   ✅ Loaded {len(y)} real outcomes | WR: {win_rate:.1%} | Market-neutral target | Recency-weighted")
                    return X, y, w_arr
            except Exception as _e:
                print(f"   ⚠️ Real outcome load failed ({_e}) — using bootstrap")

        training_file = "training_data.json"
        if os.path.exists(training_file):
            with open(training_file) as f:
                saved = json.load(f)
            X = pd.DataFrame(saved["X"])
            y = pd.Series(saved["y"])
            w = np.ones(len(y))
            return X, y, w

        print("   First run — bootstrapping model from factor research...")
        np.random.seed(42)
        n = 2000
        X_data = {
            "momentum_6m":    np.random.normal(0.05, 0.15, n),
            "momentum_12m":   np.random.normal(0.08, 0.20, n),
            "roe":            np.random.beta(2, 5, n),
            "profit_margin":  np.random.beta(1.5, 4, n),
            "earnings_yield": np.random.beta(2, 3, n) * 0.15,
            "fcf_yield":      np.random.beta(1.5, 4, n) * 0.10,
            "volatility_90d": np.random.beta(2, 5, n) * 0.6 + 0.1,
            "beta":           np.random.normal(1.0, 0.4, n).clip(0.2, 3.0),
            "rev_growth":     np.random.normal(0.08, 0.20, n),
            "earn_growth":    np.random.normal(0.10, 0.30, n),
            "div_yield":      np.random.beta(1.5, 6, n) * 0.10,
            "debt_equity":    np.random.beta(2, 3, n),
            "rs_rating":      np.random.uniform(0, 1, n),
            "sector_momentum":np.random.normal(0, 0.10, n),
        }
        X_data["vol_adj_momentum"] = np.clip(
            X_data["momentum_6m"] / np.maximum(X_data["volatility_90d"], 0.01), -5.0, 5.0)
        X = pd.DataFrame(X_data)
        score = (
            X["momentum_6m"]    * 0.20 + X["momentum_12m"]   * 0.15 +
            X["vol_adj_momentum"] * 0.10 + X["roe"]           * 0.15 +
            X["profit_margin"]  * 0.10 + X["earnings_yield"]  * 0.10 +
            X["rs_rating"]      * 0.15 + X["rev_growth"]      * 0.08 -
            X["volatility_90d"] * 0.08 - X["debt_equity"]     * 0.05 +
            np.random.normal(0, 0.05, n)
        )
        y = (score > score.median()).astype(int)
        w = np.ones(n)
        return X, y, w

    def train(self, verbose=True):
        if not HAS_XGB or not HAS_PANDAS or not HAS_SKLEARN:
            self.trained = False
            return False

        cache_file = "ml_model_cache.pkl"
        if os.path.exists(cache_file):
            try:
                import joblib as _jl, hashlib as _hl
                cached = _jl.load(cache_file)
                self.model              = cached["model"]
                self.scaler             = cached["scaler"]
                self.calibrator         = cached.get("calibrator")
                self.feature_importance = cached.get("feature_importance", {})
                _feat_hash = _hl.md5(str(ML_CONFIG["features"]).encode()).hexdigest()[:8]
                if cached.get("feature_hash") != _feat_hash:
                    os.remove(cache_file)
                    raise ValueError("feature_hash_mismatch")
                dummy = np.zeros((1, len(ML_CONFIG["features"])))
                self.scaler.transform(dummy)
                self.trained = True
                if verbose:
                    top = list(self.feature_importance.keys())[:5]
                    print(f"   OK Loaded cached model | Top features: {top}")
                return True
            except Exception as e:
                if verbose: print(f"   ⚠️ Cache incompatible ({e}) — retraining")
                try: os.remove(cache_file)
                except: pass

        if verbose: print("\n🤖 Training ML model...")
        result = self.load_training_data()
        if result is None or result[0] is None:
            return False

        X, y, sample_weights = result
        if len(y) < 50:
            return False

        from sklearn.model_selection import TimeSeriesSplit
        split = int(len(X) * 0.8)
        X_train, X_val = X.iloc[:split], X.iloc[split:]
        y_train, y_val = y.iloc[:split], y.iloc[split:]
        w_train        = sample_weights[:split]

        self.scaler = StandardScaler()
        X_train_s   = self.scaler.fit_transform(X_train)
        X_val_s     = self.scaler.transform(X_val)

        tscv = TimeSeriesSplit(n_splits=3)
        cv_aucs = []
        params = {k: v for k, v in ML_CONFIG["xgb_params"].items() if k != "use_label_encoder"}
        X_arr = self.scaler.transform(X)
        for train_idx, val_idx in tscv.split(X_arr):
            try:
                cv_model = XGBClassifier(**params)
                cv_model.fit(X_arr[train_idx], y.iloc[train_idx],
                             sample_weight=sample_weights[train_idx], verbose=False)
                cv_aucs.append(roc_auc_score(y.iloc[val_idx],
                               cv_model.predict_proba(X_arr[val_idx])[:, 1]))
            except Exception: pass

        np.random.seed(42)   # deterministic: same closed-market double-run → identical ML probs
        self.model = XGBClassifier(**params)
        self.model.fit(X_train_s, y_train, sample_weight=w_train,
                       eval_set=[(X_val_s, y_val)], verbose=False)

        val_preds = self.model.predict_proba(X_val_s)[:, 1]
        try: holdout_auc = roc_auc_score(y_val, val_preds)
        except: holdout_auc = 0.5

        try:
            from sklearn.calibration import CalibratedClassifierCV
            self.calibrator = CalibratedClassifierCV(self.model, method="isotonic", cv="prefit")
            self.calibrator.fit(X_train_s, y_train)
        except: self.calibrator = None

        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = dict(sorted(
                zip(ML_CONFIG["features"],
                    [round(float(i), 4) for i in self.model.feature_importances_]),
                key=lambda x: x[1], reverse=True))

        self.trained = True
        if verbose:
            cv_mean = float(np.mean(cv_aucs)) if cv_aucs else 0.5
            print(f"   ✅ Model trained | CV AUC: {cv_mean:.3f} | Holdout AUC: {holdout_auc:.3f}")
            print(f"   Top features: {list(self.feature_importance.keys())[:5]}")

        try:
            import joblib as _jl, hashlib as _hl2
            _jl.dump({"model": self.model, "scaler": self.scaler,
                      "calibrator": self.calibrator,
                      "feature_importance": self.feature_importance,
                      "feature_hash": _hl2.md5(str(ML_CONFIG["features"]).encode()).hexdigest()[:8]},
                     cache_file)
        except: pass
        return True

    def _build_model_input(self, features_dict):
        """Build the DataFrame that XGBoost expects. Returns (df, None) or raises."""
        mom_6m = features_dict.get("momentum_6m", 0)
        vol    = max(features_dict.get("volatility_90d", 0.02), 0.01)
        features_dict["vol_adj_momentum"] = max(min(mom_6m / vol, 5.0), -5.0)
        features_dict["sector_momentum"]  = features_dict.get("sector_momentum", 0)
        feat_order = ML_CONFIG["features"]
        row = {f: features_dict.get(f, 0) for f in feat_order}
        df  = pd.DataFrame([row])
        df["sector_encoded"] = df["sector_encoded"].astype("category")
        return df

    def predict(self, features_dict, market_regime=1):
        if not self.trained or not HAS_PANDAS:
            score = (features_dict.get("momentum_6m", 0) * 0.20 +
                     features_dict.get("roe", 0) * 0.15 +
                     features_dict.get("rs_rating", 0.5) * 0.15 +
                     features_dict.get("momentum_12m", 0) * 0.15 +
                     features_dict.get("earnings_yield", 0) * 0.10 +
                     features_dict.get("div_yield", 0) * 0.10 +
                     market_regime * 0.10 -
                     features_dict.get("volatility_90d", 0.2) * 0.05 -
                     features_dict.get("debt_equity", 0.5) * 0.05)
            return max(0.1, min(0.9, 0.5 + score))
        try:
            model_input = self._build_model_input(features_dict)
            if self.calibrator is not None:
                prob = self.calibrator.predict_proba(model_input)[0][1]
            else:
                prob = self.model.predict_proba(model_input)[0][1]
            return round(float(prob), 4)
        except: return 0.5

    def predict_raw_pair(self, features_dict, market_regime=1):
        """
        Return (calibrated_prob, xgb_raw_prob).
        Used once per run to log whether the calibrator is compressing spread.
        Falls back to (0.5, 0.5) on any error.
        """
        if not self.trained or not HAS_PANDAS:
            p = self.predict(features_dict, market_regime)
            return p, p
        try:
            model_input = self._build_model_input(features_dict)
            xgb_raw = round(float(self.model.predict_proba(model_input)[0][1]), 4)
            if self.calibrator is not None:
                calibrated = round(float(self.calibrator.predict_proba(model_input)[0][1]), 4)
            else:
                calibrated = xgb_raw
            return calibrated, xgb_raw
        except:
            return 0.5, 0.5


# ============================================================
# POSITION SIZER — Half-Kelly + Vol Targeting
# ============================================================

def get_cooldown_set(verbose=False):
    """
    Compute tiered loss cooldown set from outcomes_log.json.
    - 2 losses in 7 days  AND last loss within 3 days → 3-day block
    - 3+ losses in 14 days AND last loss within 7 days → 7-day block
    Returns set of blocked tickers and a dict {ticker: tier_label}.
    """
    import json as _j, os as _o
    from datetime import datetime as _dt, timedelta as _td
    blocked = set()
    tiers   = {}
    try:
        if not _o.path.exists("outcomes_log.json"):
            return blocked, tiers
        outcomes = _j.load(open("outcomes_log.json"))
        today  = _dt.now()
        cut7   = (today - _td(days=7)).strftime("%Y-%m-%d")
        cut14  = (today - _td(days=14)).strftime("%Y-%m-%d")
        cut3cd = (today - _td(days=3)).strftime("%Y-%m-%d")
        cut7cd = (today - _td(days=7)).strftime("%Y-%m-%d")
        losses_7d  = {}
        losses_14d = {}
        last_loss  = {}
        for o in outcomes:
            if o.get("resolved") is not True or o.get("outcome") != "LOSS":
                continue
            t  = o.get("ticker", "")
            rd = o.get("resolved_date", "") or o.get("signal_date", "")
            if not t or not rd:
                continue
            if rd >= cut14:
                losses_14d[t] = losses_14d.get(t, 0) + 1
            if rd >= cut7:
                losses_7d[t]  = losses_7d.get(t, 0) + 1
            if rd > last_loss.get(t, ""):
                last_loss[t] = rd
        for t in set(list(losses_7d) + list(losses_14d)):
            ll = last_loss.get(t, "")
            if losses_14d.get(t, 0) >= 3 and ll >= cut7cd:
                blocked.add(t)
                tiers[t] = "7d"
            elif losses_7d.get(t, 0) >= 2 and ll >= cut3cd:
                blocked.add(t)
                tiers[t] = "3d"
        if blocked and verbose:
            parts = sorted(f"{t}({tiers[t]})" for t in blocked)
            print(f"   🧊 Cooldown: {', '.join(parts)}")
    except Exception:
        pass

    # ── Read cooldown_flags.json (loss-streak flags) ──────────────────────
    # Without this: loss-streak flags written by run_daily.py bypass ML
    # engine's cooldown check and reach position sizing.
    try:
        import json as _jf2, datetime as _dttf2
        _today_str2 = _dttf2.date.today().isoformat()
        _flags2 = _jf2.load(open("cooldown_flags.json"))
        for _ftk2, _fdata2 in _flags2.items():
            if _fdata2.get("expires", "") >= _today_str2:
                blocked.add(_ftk2)
                tiers[_ftk2] = "flag"
    except FileNotFoundError:
        pass
    except Exception:
        pass

    # ── Read long_cooldowns.json (90-day rolling cooldown with auto-renew) ─
    # Replaces the old permanent_exclusions.json concept. Each ticker has a
    # blocked_until date; on expiry, rolling WR from last 20 resolved picks
    # determines renewal (WR < 35%) or clearance (WR >= 35%).
    try:
        import json as _jlc, datetime as _dtlc, os as _olc
        if _olc.path.exists("long_cooldowns.json"):
            _lc_data = _jlc.load(open("long_cooldowns.json"))
            _today_lc     = _dtlc.date.today()
            _today_str_lc = _today_lc.isoformat()
            _lc_modified  = False
            _lc_to_remove = []

            # Load outcomes for rolling WR check on expired entries
            _lc_outcomes = []
            try:
                _lc_outcomes = _jlc.load(open("outcomes_log.json"))
            except Exception:
                pass

            for _tkr_lc, _entry_lc in list(_lc_data.items()):
                _blocked_until_lc = _entry_lc.get("blocked_until", "")
                if _today_str_lc <= _blocked_until_lc:
                    # Still within the 90-day window — block unconditionally
                    blocked.add(_tkr_lc)
                    tiers[_tkr_lc] = "long_cd"
                elif _entry_lc.get("auto_renew", False):
                    # Block expired — check rolling WR from last 20 resolved picks
                    _threshold_lc = _entry_lc.get("renew_threshold_WR", 0.35)
                    _resolved_lc  = sorted(
                        [o for o in _lc_outcomes
                         if o.get("ticker") == _tkr_lc
                         and o.get("resolved") is True
                         and o.get("outcome") in ("WIN", "LOSS", "FLAT")],
                        key=lambda x: x.get("resolved_date") or x.get("signal_date") or ""
                    )[-20:]
                    _rwr_lc = (
                        sum(1 for o in _resolved_lc if o["outcome"] == "WIN") / len(_resolved_lc)
                        if _resolved_lc else 0.0
                    )
                    if _rwr_lc < _threshold_lc:
                        _new_until_lc = (_today_lc + _dtlc.timedelta(days=90)).isoformat()
                        _lc_data[_tkr_lc]["blocked_until"] = _new_until_lc
                        blocked.add(_tkr_lc)
                        tiers[_tkr_lc] = "long_cd"
                        _lc_modified = True
                        print(f"  🔄 Long cooldown renewed: {_tkr_lc} "
                              f"(rolling WR {_rwr_lc:.0%} < {_threshold_lc:.0%} threshold, "
                              f"blocked until {_new_until_lc})")
                    else:
                        _lc_to_remove.append(_tkr_lc)
                        print(f"  ✅ Long cooldown cleared: {_tkr_lc} "
                              f"(rolling WR {_rwr_lc:.0%} >= {_threshold_lc:.0%} threshold, "
                              f"returning to universe)")

            for _tkr_rm in _lc_to_remove:
                del _lc_data[_tkr_rm]
                _lc_modified = True

            if _lc_modified:
                with open("long_cooldowns.json", "w") as _lc_f:
                    _jlc.dump(_lc_data, _lc_f, indent=2)
    except FileNotFoundError:
        pass
    except Exception:
        pass

    return blocked, tiers


def _apply_sector_cap(picks, screener_picks, max_per_sector=2, excluded_tickers=frozenset()):
    """
    Enforce sector diversity in final basket.
    If >max_per_sector picks share a sector, replace excess with next-best
    pick from an under-represented sector.

    Sector is taken from pick.get("sector") or pick.get("data",{}).get("sector").
    Picks without sector info are treated as "Unknown" — count toward no cap.
    """
    # Sector normalization map — yfinance returns inconsistent strings
    # e.g. "Financial Services", "Banks", "Insurance" all = same sector risk
    _SECTOR_NORM = {
        "financial services": "Financials",
        "financials":         "Financials",
        "banks":              "Financials",
        "insurance":          "Financials",
        "asset management":   "Financials",
        "capital markets":    "Financials",
        "diversified financials": "Financials",
        "real estate":        "Real Estate",
        "reits":              "Real Estate",
        "reit":               "Real Estate",
        "real estate investment trusts": "Real Estate",
        "energy":             "Energy",
        "oil & gas":          "Energy",
        "oil and gas":        "Energy",
        "utilities":          "Utilities",
        "consumer discretionary": "Consumer",
        "consumer staples":   "Consumer",
        "technology":         "Technology",
        "information technology": "Technology",
        "communication services": "Communication",
        "telecommunications": "Communication",
        "telecom":            "Communication",
        "health care":        "Healthcare",
        "healthcare":         "Healthcare",
        "pharmaceuticals":    "Healthcare",
        "biotechnology":      "Healthcare",
        "industrials":        "Industrials",
        "materials":          "Materials",
    }

    def _get_sector(pick):
        s = pick.get("sector") or pick.get("data", {}).get("sector", "")
        if not (s or "").strip():
            s = _TICKER_SECTOR_OVERRIDE.get(pick.get("ticker", ""), "")
        s = (s or "Unknown").strip()
        return _SECTOR_NORM.get(s.lower(), s)

    # Count sectors in current basket
    from collections import Counter
    sector_counts = Counter(_get_sector(p) for p in picks)

    # Sort by score desc, ticker asc as tiebreaker — fully deterministic even
    # when two picks share the same composite score across all cap rounds.
    picks = sorted(picks, key=lambda x: (-(x.get("score", 0) or 0), x.get("ticker", "") or ""))

    # Identify over-represented picks (keep first max_per_sector, flag rest)
    seen = Counter()
    kept   = []
    excess = []
    for pick in picks:
        s = _get_sector(pick)
        if s == "Unknown" or seen[s] < max_per_sector:
            kept.append(pick)
            seen[s] += 1
        else:
            excess.append(pick)

    if not excess:
        return picks  # no concentration issue

    # Build reserve pool from all screener picks not already in basket
    # and not on any cooldown / news-penalty exclusion list.
    # excluded_tickers must be the full consolidated exclusion set so that
    # tickers already filtered upstream cannot re-enter here.
    basket_tickers = {p.get("ticker") for p in picks}
    all_candidates = []
    _sec_cap_excluded = 0
    for group_name in ["TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3",
                       "FHSA_top5", "conviction_picks"]:
        for p in screener_picks.get(group_name, []):
            _tkr = p.get("ticker")
            if _tkr in basket_tickers:
                continue
            if _tkr in excluded_tickers:
                _sec_cap_excluded += 1
                continue
            all_candidates.append(p)
    if _sec_cap_excluded:
        print(f"  🏛  Sector cap reserve: filtered {_sec_cap_excluded} excluded tickers")

    # Sort reserve pool by score descending
    all_candidates.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Fill excess slots with best candidates from under-represented sectors
    filled = list(kept)
    for candidate in all_candidates:
        if len(filled) >= len(picks):
            break
        s = _get_sector(candidate)
        if s == "Unknown" or seen[s] < max_per_sector:
            filled.append(candidate)
            seen[s] += 1
            basket_tickers.add(candidate.get("ticker"))

    if len(filled) < len(picks):
        # Not enough diversified replacements — keep original excess to maintain basket size
        filled.extend(excess[:len(picks) - len(filled)])

    if len(excess) > 0:
        removed  = [p.get("ticker") for p in excess[:len(excess)]]
        added    = [p.get("ticker") for p in filled[len(kept):]]
        if removed or added:
            import sys
            print(f"  🏛  Sector cap: removed {removed}, added {added} (max {max_per_sector}/sector)",
                  file=sys.stdout)

    return filled[:len(picks)]


# ─── ACCOUNT UNIVERSE FILTERS ─────────────────────────────────────────────────
# Shared by ML stock routing and ETF routing so the two can't drift apart.
_UNIVERSE_FILTERS = {
    "ALL":             lambda ticker: True,
    "US_ONLY":         lambda ticker: not ticker.endswith(".TO"),
    "CA_CONSERVATIVE": lambda ticker: ticker.endswith(".TO"),
}
# High-volatility US thematics excluded from CA_CONSERVATIVE (FHSA)
_FHSA_AVOID = {"BOTZ", "SMH", "SKYY", "CIBR", "ITA", "SHLD", "PPA", "QTUM", "ARKG", "BLOK"}


def load_accounts():
    """Load accounts.json. Returns [] (graceful fallback) if absent or malformed."""
    try:
        with open("accounts.json") as f:
            accounts = json.load(f)
        return accounts if isinstance(accounts, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def compute_target_weights(picks, market_regime, sector_sentiment=None,
                            win_rate_data=None, verbose=True):
    """
    Pure weight computation — no dollar amounts.
    All Kelly/vol-target/ML-proportional/concentration-cap logic lives here.
    Dollar conversion belongs in render_allocations().

    Returns list of {ticker, weight, weight_pct, ml_prob, kelly_wt, vol_adj, score, price}.
    `weight` is the raw fraction; `weight_pct` is weight * 100 rounded for display.
    """
    if not picks:
        return []

    picks = [p for p in picks if (p.get("score") or 0) >= 60]
    if not picks:
        return []

    _cooldown_set, _ = get_cooldown_set(verbose=verbose)
    picks = [p for p in picks if p.get("ticker", "") not in _cooldown_set]
    if not picks:
        if verbose: print("   ⚠️ All picks on cooldown — no positions sized")
        return []

    cfg = ML_CONFIG

    regime_equity_pct = 1.0 - market_regime.get("cash_pct", 0.0)
    n_picks  = min(len(picks), cfg["max_positions"])
    base_wt  = 1.0 / n_picks

    if verbose:
        print(f"\n⚖️  TARGET WEIGHTS ({n_picks} picks):")
        print(f"   Regime: {market_regime['regime']} | "
              f"Equity: {round(regime_equity_pct*100)}% | "
              f"Cash: {round((1 - regime_equity_pct)*100)}%")

    # ── SECTOR BLOCK ──────────────────────────────────────────────────────────
    sector_sentiment = sector_sentiment or {}
    SECTOR_MAP_BLOCK = {
        "Communication Services": "TELECOM",
        "Consumer Discretionary": "CONSUMER_DISCRETIONARY",
        "Consumer Cyclical":      "CONSUMER_DISCRETIONARY",
        "Materials":              "CANADIAN_MATERIALS",
    }
    sector_blocked = set()
    for p in picks[:n_picks]:
        yf_sector  = p.get("data", {}).get("sector", "") or p.get("sector", "")
        news_sector = SECTOR_MAP_BLOCK.get(yf_sector)
        if news_sector and sector_sentiment:
            net = sector_sentiment.get(news_sector, {}).get("net_score", 0)
            if net <= -200:
                sector_blocked.add(p["ticker"])
                if verbose:
                    print(f"   🚫 Sector block: {p['ticker']} ({news_sector} net:{net})")

    # ── HALF-KELLY WEIGHTS ────────────────────────────────────────────────────
    def score_to_kelly_wt(score, wr_data=None):
        # Static calibration — used when live data absent or too thin (n < 10)
        if score >= 90:   p, aw, al = 0.492, 0.70, 1.0
        elif score >= 75: p, aw, al = 0.595, 1.10, 1.0
        elif score >= 60: p, aw, al = 0.658, 1.80, 1.0
        else:             p, aw, al = 0.556, 1.10, 1.0
        if wr_data and wr_data.get("by_score_tier"):
            t = wr_data["by_score_tier"]
            if score >= 90:   d = t.get("90-100", {})
            elif score >= 75: d = t.get("75-89",  {})
            elif score >= 60: d = t.get("60-74",  {})
            else:             d = t.get("below-60", {})
            _aw = d.get("avg_win")    # mean return on winning picks
            _al = d.get("avg_loss")   # mean abs-return on losing picks
            _n  = d.get("count", 0)
            _nw = round(_n * d.get("win_rate", 0) / 100)
            _nl = _n - _nw
            if _aw and _al and _al > 0 and _nw >= 10 and _nl >= 10:
                p  = d.get("win_rate", 50) / 100
                aw = _aw   # proper odds ratio numerator
                al = _al   # proper odds ratio denominator
            else:
                print(f"    [Kelly] thin data (nw={_nw}, nl={_nl}) → static fallback")
        b = aw / al
        kelly = (p * b - (1 - p)) / b
        return max(0.0, kelly * 0.50)

    _score_hist = {}
    _outcomes_lookup = {}
    try:
        import json as _jout
        _raw_outcomes = _jout.load(open("outcomes.json"))
        for _o in _raw_outcomes:
            _tk = _o.get("ticker", "")
            if _tk:
                _outcomes_lookup.setdefault(_tk, []).append(_o)
    except Exception:
        pass
    try:
        import json as _j
        _score_hist = _j.load(open("score_history.json"))
    except Exception:
        pass

    def _trend_adjusted_score(pick, hist, outcomes_lookup=None):
        base   = pick.get("score", 70)
        ticker = pick.get("ticker", "")
        records = hist.get(ticker, [])
        if records:
            recent = sorted(records, key=lambda x: x.get("date", ""), reverse=True)
            latest_score = round(float(recent[0].get("score", base)), 1)
            return min(base, latest_score)
        if outcomes_lookup:
            prior = outcomes_lookup.get(ticker, [])
            if prior:
                prior_sorted = sorted(prior, key=lambda x: x.get("signal_date", ""), reverse=True)
                prior_score  = prior_sorted[0].get("score", base)
                if prior_score and prior_score < base:
                    return float(prior_score)
        if base > 80:
            return 75.0
        return base

    def _ml_edge_multiplier(ml_prob):
        p = float(ml_prob or 0)
        if p > 0.85:  return 0.30
        if p >= 0.60: return 1.50
        if p >= 0.40: return 1.00
        if p >= 0.20: return 0.60
        return 0.30

    raw_kelly_wts = [score_to_kelly_wt(
                         _trend_adjusted_score(p, _score_hist, _outcomes_lookup),
                         win_rate_data)
                     for p in picks[:n_picks]]

    kelly_wts = [raw_kelly_wts[i] * _ml_edge_multiplier(picks[i].get("ml_prob", 0.5))
                 for i in range(n_picks)]

    if verbose:
        for i, p in enumerate(picks[:n_picks]):
            _mult = _ml_edge_multiplier(p.get("ml_prob", 0.5))
            if abs(_mult - 1.0) > 0.05:
                _tag = "✅" if _mult > 1.0 else "⚠️ "
                print(f"   {p['ticker']:<10} ml_edge_mult={_mult:.2f}× {_tag}  "
                      f"(ml_prob={p.get('ml_prob', 0):.2f})")

    total_kelly      = sum(kelly_wts)
    n_positive_kelly = sum(1 for w in kelly_wts if w > 0)

    if total_kelly > 0:
        raw_norm_kelly = [w / total_kelly for w in kelly_wts]
        if n_positive_kelly < n_picks / 2:
            equal_kelly = [base_wt] * n_picks
            blend_ratio = n_positive_kelly / n_picks
            norm_kelly  = [blend_ratio * raw_norm_kelly[i] + (1 - blend_ratio) * equal_kelly[i]
                           for i in range(n_picks)]
            if verbose and n_positive_kelly <= 2:
                print(f"   ℹ️ Kelly degraded ({n_positive_kelly}/{n_picks} picks have edge)"
                      f" — blending {blend_ratio:.0%} Kelly + {1-blend_ratio:.0%} equal")
        else:
            norm_kelly = raw_norm_kelly
    else:
        norm_kelly = [base_wt] * n_picks
        if verbose: print("   ℹ️ Kelly weights zero — falling back to equal weights")

    # ── VOLATILITY TARGETING ──────────────────────────────────────────────────
    TARGET_VOL = 0.20
    vols = [max(0.05, min(0.80, float(p.get("data", {}).get("volatility_90d", 0.2) or 0.2)))
            for p in picks[:n_picks]]
    vol_wts   = [(TARGET_VOL / v) * base_wt for v in vols]
    total_vol = sum(vol_wts)
    norm_vol  = [w / total_vol for w in vol_wts] if total_vol > 0 else [base_wt] * n_picks

    # ── BLEND 40% Kelly + 60% Vol ─────────────────────────────────────────────
    blended = [0.40 * norm_kelly[i] + 0.60 * norm_vol[i] for i in range(n_picks)]
    total_b = sum(blended)
    norm_b  = [w / total_b for w in blended] if total_b > 0 else [base_wt] * n_picks

    # ── ML-PROPORTIONAL WEIGHTING ────────────────────────────────────────────
    ml_probs = [p.get("ml_prob", 0.5) for p in picks[:n_picks]]
    total_ml = sum(ml_probs) or 1.0
    norm_ml  = [prob / total_ml for prob in ml_probs]

    MAX_SINGLE = max(0.20, 1.5 / n_picks)
    final_wts  = []
    for i in range(n_picks):
        if picks[i]["ticker"] in sector_blocked:
            final_wts.append(0.0)
        elif kelly_wts[i] == 0.0:
            final_wts.append(base_wt * 0.50)
        else:
            w = 0.33 * norm_kelly[i] + 0.33 * norm_vol[i] + 0.33 * norm_ml[i]
            w = min(MAX_SINGLE, w)
            final_wts.append(w)

    total_f = sum(final_wts)
    if total_f == 0:
        if verbose: print("   ℹ️ All Kelly=0 — falling back to equal weight")
        final_wts = [base_wt if picks[i]["ticker"] not in sector_blocked else 0.0
                     for i in range(n_picks)]
        total_f = sum(final_wts)

    MAX_HARD = max(0.20, 1.5 / n_picks)
    for _iter in range(6):
        _nz = sum(w for w in final_wts if w > 0)
        if _nz == 0: break
        final_wts = [w / _nz for w in final_wts]
        _excess   = sum(max(0.0, w - MAX_HARD) for w in final_wts)
        if _excess < 0.0005: break
        _n_unc = sum(1 for w in final_wts if 0 < w < MAX_HARD)
        _boost  = _excess / _n_unc if _n_unc > 0 else 0
        final_wts = [min(MAX_HARD, w) if w > 0 else 0.0 for w in final_wts]
        final_wts = [min(MAX_HARD, w + _boost) if 0 < w < MAX_HARD else w
                     for w in final_wts]

    # Hard concentration cap for thin baskets — surplus stays as cash (no re-norm)
    CONC_CAP = 0.25
    if n_picks < 6:
        capped = [min(w, CONC_CAP) if w > 0 else 0.0 for w in final_wts]
        if capped != final_wts:
            total_deployed = sum(capped)
            if verbose:
                print(f"   ⚠️  Concentration cap ({n_picks} picks): "
                      f"max weight → {CONC_CAP*100:.0f}% | "
                      f"{total_deployed*100:.0f}% deployed, "
                      f"{(1-total_deployed)*100:.0f}% reserved as cash")
            final_wts = capped

    # ── NEW-TICKER PROBATION ─────────────────────────────────────────────────
    # Tickers with 0 prior entries in outcomes_log.json get weight capped at
    # PROBATION_CAP (50%) of their computed allocation. Sector-gate trust is
    # earned at ticker level, not inherited — scout-added names enter here.
    _outcomes_counts = load_outcomes_ticker_counts()
    _probation_applied = []
    for i in range(n_picks):
        _tkr = picks[i]["ticker"]
        if _outcomes_counts.get(_tkr, 0) == 0 and final_wts[i] > 0:
            _capped_wt = final_wts[i] * PROBATION_CAP
            if verbose:
                print(f"   🔰 PROBATION ({_tkr}): weight capped "
                      f"{final_wts[i]*100:.1f}% → {_capped_wt*100:.1f}% "
                      f"(first occurrence — 0 outcomes log entries)")
            final_wts[i] = _capped_wt
            _probation_applied.append(_tkr)

    result = []
    for i, pick in enumerate(picks[:n_picks]):
        wt = final_wts[i]
        result.append({
            "ticker":     pick["ticker"],
            "weight":     wt,                    # raw fraction — used by render_allocations
            "weight_pct": round(wt * 100, 2),
            "ml_prob":    round(ml_probs[i], 3),
            "vol_adj":    round(vols[i], 3),
            "kelly_wt":   round(kelly_wts[i], 3),
            "score":      pick.get("score", 70),
            "price":      float(pick.get("data", {}).get("price", 0) or 0),
            "probation":  pick["ticker"] in _probation_applied,
        })

    if verbose:
        print(f"   {'Ticker':<12} {'Weight':>7}  {'ML':>5}  {'Kelly':>7}")
        for pos in result:
            bar = "▮" * int(pos["weight_pct"] / 5)
            print(f"   {pos['ticker']:<12} {pos['weight_pct']:>6.1f}%  "
                  f"{pos['ml_prob']:.2f}  {pos['kelly_wt']:.3f}  {bar}")

    return result


def render_allocations(target_weights, account, market_regime,
                        current_drawdown=0.0, min_position=250.0, verbose=True):
    """
    Convert weight fractions to dollar amounts for one account.
    Applies: universe filter, regime equity%, drawdown reduction, min_position floor.

    Returns list of {ticker, weight_pct, dollar_amt, ml_prob, kelly_wt, vol_adj, score}.
    When universe is ALL and no picks are filtered, dollar_amt == deployable * weight
    to the cent — matching the legacy calculate_position_sizes output exactly.
    """
    capital = float(account.get("capital", 0))
    if capital <= 0:
        return []

    acct_name  = account.get("name", "ACCOUNT")
    universe   = account.get("universe", "ALL")
    max_equity = float(account.get("max_equity", 1.0))

    # Universe filter
    base_filter = _UNIVERSE_FILTERS.get(universe, _UNIVERSE_FILTERS["ALL"])
    if universe == "CA_CONSERVATIVE":
        eligible = [w for w in target_weights
                    if base_filter(w["ticker"]) and w["ticker"] not in _FHSA_AVOID]
    else:
        eligible = [w for w in target_weights if base_filter(w["ticker"])]

    if not eligible:
        if verbose:
            print(f"   ⚠️ [{acct_name}] No picks eligible for universe '{universe}'")
        return []

    # Regime equity allocation
    regime_equity_pct = 1.0 - market_regime.get("cash_pct", 0.0)
    regime_equity_pct = min(regime_equity_pct, max_equity)

    # Drawdown reduction
    cfg = ML_CONFIG
    dd_multiplier = 1.0
    if current_drawdown > cfg["drawdown_reduction_trigger"]:
        dd_multiplier = 1.0 - cfg["drawdown_reduction_amount"]
        if verbose:
            print(f"   ⚠️ [{acct_name}] Drawdown {current_drawdown*100:.1f}% — reducing by 30%")

    deployable = capital * regime_equity_pct * dd_multiplier

    if verbose:
        print(f"\n   💰 {acct_name} (${capital:,.0f}"
              + (f" | max_equity: {max_equity*100:.0f}%" if max_equity < 1.0 else "")
              + f" | Equity: {round(regime_equity_pct*100)}%"
              + f"): deploying ${deployable:,.0f}")

    # Weight scaling: re-normalize only when universe filter removed some picks.
    # When universe=ALL, use raw fractions so the cash from CONC_CAP is preserved.
    if len(eligible) < len(target_weights):
        total_wt = sum(w["weight"] for w in eligible) or 1.0
        wt_scale = 1.0 / total_wt
    else:
        wt_scale = 1.0

    allocations = []
    for w in eligible:
        norm_weight = w["weight"] * wt_scale
        dollar_amt  = round(deployable * norm_weight, 2)
        allocations.append({
            **w,
            "weight_pct": round(norm_weight * 100, 2),
            "dollar_amt": dollar_amt,
        })

    # Min-position floor: $250 or 1 share (whichever larger), then re-normalize
    dropped = []
    kept    = []
    for a in allocations:
        price = a.get("price", 0)
        floor = max(min_position, price) if price > 0 else min_position
        if 0 < a["dollar_amt"] < floor:
            dropped.append(a["ticker"])
        else:
            kept.append(a)

    if dropped:
        if verbose:
            print(f"   ⚠️ [{acct_name}] Min-floor ${min_position:.0f} dropped: {dropped}")
        total_kept_wt = sum(a["weight"] * wt_scale for a in kept) or 1.0
        for a in kept:
            nw = a["weight"] * wt_scale / total_kept_wt
            a["weight_pct"] = round(nw * 100, 2)
            a["dollar_amt"]  = round(deployable * nw, 2)
        allocations = kept

    if verbose:
        for pos in allocations:
            print(f"   {pos['ticker']:<12} {pos['weight_pct']:>5.1f}%  "
                  f"${pos['dollar_amt']:>8,.0f}  "
                  f"ML: {pos['ml_prob']:.2f}  Kelly: {pos['kelly_wt']:.3f}")

    return allocations


def calculate_position_sizes(picks, portfolio_value, market_regime, current_drawdown=0.0,
                              max_equity=1.0, verbose=True, sector_sentiment=None,
                              win_rate_data=None, **kwargs):
    """
    Backward-compatible wrapper: compute weights then render for a single account.

    For multi-account sizing use compute_target_weights() + render_allocations()
    directly (as run_ml_engine does when accounts.json is present).
    """
    weights = compute_target_weights(
        picks, market_regime,
        sector_sentiment=sector_sentiment,
        win_rate_data=win_rate_data,
        verbose=verbose,
    )
    if not weights:
        return []
    account = {
        "name":       "TFSA",
        "capital":    portfolio_value,
        "universe":   "ALL",
        "max_equity": max_equity,
    }
    return render_allocations(
        weights, account, market_regime,
        current_drawdown=current_drawdown,
        verbose=verbose,
    )


# ============================================================
# BACKTEST SUMMARY
# ============================================================

def run_backtest_summary(regime, ml_predictor, verbose=True):
    factor_returns = {
        "momentum":    {"annual_ret": 0.122, "sharpe": 0.62, "max_dd": -0.38},
        "quality":     {"annual_ret": 0.108, "sharpe": 0.58, "max_dd": -0.32},
        "combined":    {"annual_ret": 0.138, "sharpe": 0.74, "max_dd": -0.29},
        "with_regime": {"annual_ret": 0.142, "sharpe": 0.88, "max_dd": -0.19},
        "sp500_bench": {"annual_ret": 0.104, "sharpe": 0.51, "max_dd": -0.51},
    }
    regime_name  = regime.get("regime", "UNKNOWN")
    regime_bonus = {"BULL": 1.15, "RECOVERY": 0.95, "CAUTION": 0.80, "BEAR": 0.60}.get(regime_name, 1.0)
    base = factor_returns["with_regime"]
    est_low  = round((base["annual_ret"] * regime_bonus - 0.04) * 100, 1)
    est_high = round((base["annual_ret"] * regime_bonus + 0.06) * 100, 1)

    result = {
        "factor_performance": factor_returns,
        "current_regime":     regime_name,
        "estimated_annual_return_range": f"{est_low}% to {est_high}%",
        "estimated_sharpe":   round(base["sharpe"] * regime_bonus, 2),
        "estimated_max_dd":   f"{round(base['max_dd'] * 100, 1)}%",
        "vs_benchmark": {"sp500_hist_avg": "10.4%"},
        "honest_limitations": ["Past factor performance does not guarantee future results"],
    }
    if verbose:
        print(f"\n📊 BACKTEST SUMMARY")
        print(f"   Strategy estimated return: {result['estimated_annual_return_range']}")
        print(f"   Estimated Sharpe:          {result['estimated_sharpe']}")
        print(f"   Estimated Max Drawdown:    {result['estimated_max_dd']}")
        print(f"   vs S&P 500 historical avg: 10.4%")
        print(f"\n   ⚠️  {result['honest_limitations'][0]}")
    return result


# ============================================================
# MAIN ML RUN
# ============================================================

def run_ml_engine(screener_picks, rs_ratings, verbose=True, max_equity=1.0,
                  sector_sentiment=None, win_rate_data=None):
    """
    Full ML engine run with score smoothing + Kelly sizing.
    win_rate_data: pass brief['win_rate'] from outcome_tracker for live Kelly calibration.
    """
    if verbose:
        print("\n" + "="*55)
        print("  ML ENGINE")
        print("="*55)

    # Load smooth cache
    _load_smooth_cache()

    regime    = get_market_regime(verbose=verbose)
    predictor = StockMLPredictor()
    predictor.train(verbose=verbose)

    regime_num = 1 if regime["regime"] in ("BULL", "RECOVERY") else 0
    all_picks  = (
        screener_picks.get("FHSA_top5", []) +
        screener_picks.get("TFSA_growth_top5", []) +
        screener_picks.get("TFSA_income_top5", []) +
        screener_picks.get("TFSA_swing_top3", [])
    )

    if verbose: print(f"\n🤖 Scoring {len(all_picks)} picks with ML...")

    smoothed_count  = 0
    _raw_calib_log  = []   # [(ticker, xgb_raw, calibrated, smoothed)] — logged once after loop
    for pick in all_picks:
        ticker     = pick["ticker"]
        stock_data = pick.get("data", {})
        rs         = rs_ratings.get(ticker, {}).get("rs_rating", 50) if rs_ratings else 50

        features = build_features_for_stock(ticker, stock_data, rs)

        if features:
            calibrated, xgb_raw = predictor.predict_raw_pair(features, market_regime=regime_num)
            # ── Score smoothing: 3-day EMA to dampen single-day spikes ──────
            smoothed_prob = smooth_ml_prob(ticker, calibrated, alpha=0.4)
            if abs(smoothed_prob - calibrated) > 0.03:
                smoothed_count += 1
            pick["ml_prob"]     = smoothed_prob
            pick["ml_prob_raw"] = calibrated   # post-calibration, pre-smoothing
            pick["ml_prob_xgb"] = xgb_raw      # raw XGBoost output (pre-calibration)
            _raw_calib_log.append((ticker, xgb_raw, calibrated, smoothed_prob))
        else:
            pick["ml_prob"]     = 0.5
            pick["ml_prob_raw"] = 0.5
            pick["ml_prob_xgb"] = 0.5
            _raw_calib_log.append((ticker, 0.5, 0.5, 0.5))

        pick["ml_signal"] = ("🔥 STRONG BUY"  if pick["ml_prob"] >= 0.70 else
                             "✅ BUY"          if pick["ml_prob"] >= 0.58 else
                             "📊 NEUTRAL"      if pick["ml_prob"] >= 0.45 else
                             "⚠️ WEAK")
        ml_score_adj = round((pick["ml_prob"] - 0.5) * 20)
        pick["score"] = max(0, min(100, pick["score"] + ml_score_adj))
        time.sleep(0.1)

    if verbose and smoothed_count:
        print(f"   📊 Score smoothing: {smoothed_count} picks dampened (3-day EMA)")

    # ── RAW vs CALIBRATED PROB LOG (once per run) ─────────────────────────────
    # p90=0.500 exactly → isotonic calibrator may be clipping/saturating.
    # If xgb_raw has spread that calibration is destroying, refit isotonic at
    # next retrain on real outcomes — do NOT hand-patch the calibrator.
    if verbose and _raw_calib_log:
        _xgb_vals  = [r[1] for r in _raw_calib_log]
        _cal_vals  = [r[2] for r in _raw_calib_log]
        _xgb_spread = max(_xgb_vals) - min(_xgb_vals)
        _cal_spread = max(_cal_vals) - min(_cal_vals)
        print(f"\n  📐 Raw XGB spread={_xgb_spread:.3f} "
              f"[{min(_xgb_vals):.3f}–{max(_xgb_vals):.3f}]  "
              f"Calibrated spread={_cal_spread:.3f} "
              f"[{min(_cal_vals):.3f}–{max(_cal_vals):.3f}]")
        if _xgb_spread > _cal_spread + 0.05:
            print(f"  ⚠️  Calibrator compressing spread by "
                  f"{(_xgb_spread - _cal_spread):.3f} — isotonic needs refit on real outcomes")
        print(f"  {'Ticker':<12} {'XGB-raw':>8} {'Calib':>8} {'Smoothed':>9}")
        for tk, xr, ca, sm in sorted(_raw_calib_log, key=lambda x: -x[2]):
            marker = " ⚠️" if abs(xr - ca) > 0.05 else ""
            print(f"  {tk:<12} {xr:>8.4f} {ca:>8.4f} {sm:>9.4f}{marker}")

    # Save updated smooth cache
    _save_smooth_cache()

    # ── ML GATE (single instance, shared with conviction path via ml_results) ──
    # Build gate from ALL scored picks (not just TFSA — full distribution matters).
    _all_probs = [p.get("ml_prob") for p in all_picks if p.get("ml_prob") is not None]
    gate = MLGate(_all_probs, verbose=verbose)

    tfsa_picks = (screener_picks.get("TFSA_growth_top5", []) +
                  screener_picks.get("TFSA_income_top5", []))

    # Consolidated exclusion — applied once here so sector cap and ML gate
    # both see a fully filtered basket from the start.
    # Covers: outcome-based cooldown tiers + loss-streak flags + news-penalised picks.
    _cd_basket, _ = get_cooldown_set(verbose=False)
    tfsa_picks = [
        p for p in tfsa_picks
        if p.get("ticker", "") not in _cd_basket          # cooldown / loss-streak
        and (p.get("news_adjustment", 0) or 0) >= 0       # no negative news signal
    ]

    # ── Sector blocks at score ≥ 75 ─────────────────────────────────────
    # Evidence-backed blocks applied below the 90-tier gate:
    #   MATERIALS  ≥75: PF=0.17 at 90+ extends down to prevent accumulation at 75-89
    #   HEALTHCARE ≥75: PF=0.39 at 90+ (evidence now extends block down to 75)
    #   REIT       ≥75: PF=0.46 at 90+
    #   TELECOM    ≥75: PF=0.20 at 90+
    # CONSUMER 60-74: -8pt penalty only (PF at 60-74 = tbd, pending retrain)
    # MATERIALS 60-74: PF=1.40 — do NOT block here
    _BLOCK_75 = {'MATERIALS', 'HEALTHCARE', 'REIT', 'TELECOM'}
    _pre_filter_tickers = {p.get("ticker") for p in tfsa_picks}   # snapshot before any removal
    _pre_gate_filtered = []
    for _p in tfsa_picks:
        _sec   = (_p.get("sector") or (_p.get("data") or {}).get("sector", "") or "").strip().upper()
        _score = _p.get("score", 0) or 0
        if _sec in _BLOCK_75 and _score >= 75:
            if verbose:
                print(f"  🚫 Sector≥75 block ({_sec}): {_p.get('ticker')} (score {_score:.0f})")
            continue
        if _sec == "CONSUMER" and 60 <= _score < 75:
            _p["score"] = max(0, _score - 8)
            _p.setdefault("flags", []).append("⚠️ Consumer 60-74 penalty: -8pts")
        _pre_gate_filtered.append(_p)
    tfsa_picks = _pre_gate_filtered
    _block75_removed = _pre_filter_tickers - {p.get("ticker") for p in tfsa_picks}

    # ── Sector diversity cap ──────────────────────────────────────────────
    # Max 2 picks per sector in the final basket.
    # Problem it solves: JPM + TD.TO + REI-UN.TO all land in financials
    # under CAUTIOUS macro — 3 correlated positions, stress test shows fragility.
    # When a sector is over-represented, excess picks are replaced by the
    # next-best scoring pick from a different sector.
    _pre_cap_tickers = {p.get("ticker") for p in tfsa_picks}
    tfsa_picks = _apply_sector_cap(tfsa_picks, screener_picks, max_per_sector=2,
                                   excluded_tickers=_cd_basket)
    _cap_removed = _pre_cap_tickers - {p.get("ticker") for p in tfsa_picks}
    # Union of all tickers removed BEFORE gate evaluation — reserve pool must
    # never re-admit these (fixes C-entering-while-on-sector-cap-removal-list bug).
    _pre_gate_excluded = _block75_removed | _cap_removed

    # ── Sector-first gate on 90-100 tier ─────────────────────────────────
    # Sector performance at score≥90 (2,019 picks, July 2026):
    #   ALLOW:  ENERGY N=157 PF=2.30 | BANKS N=129 PF=2.89 | FINANCIALS N=69 PF=6.31
    #   BLOCK:  MATERIALS N=114 PF=0.17 | TELECOM N=65 PF=0.20 | HEALTHCARE N=48 PF=0.39
    #           REIT N=57 PF=0.46 | CONSUMER N=65 PF=0.89
    #   OTHER:  Fall through to ML gate (gate_engine.MLGate — compression-aware)
    SECTOR_ALLOW      = {'ENERGY', 'BANKS', 'FINANCIALS'}
    SECTOR_BLOCK      = {'MATERIALS', 'TELECOM', 'HEALTHCARE', 'REIT', 'CONSUMER'}
    ML_GATE_SCORE_MIN = 90
    basket_tickers    = {p.get("ticker") for p in tfsa_picks}
    gated_out         = []
    passed            = []
    _gate_decisions   = {}   # {ticker: bool} — saved at end for hysteresis tomorrow

    for pick in tfsa_picks:
        score   = pick.get("score", 0) or 0
        ml_prob = pick.get("ml_prob", 0.5) or 0.5
        ticker  = pick.get("ticker", "")
        _raw_sec = (pick.get("sector") or "").strip()
        if not _raw_sec:
            _raw_sec = _TICKER_SECTOR_OVERRIDE.get(ticker, "")
        sector  = _raw_sec.upper()

        if score < ML_GATE_SCORE_MIN:
            passed.append(pick)
            _gate_decisions[ticker] = True
            continue

        if sector in SECTOR_ALLOW:
            passed.append(pick)
            _gate_decisions[ticker] = True
            if verbose:
                print(f"  ✅ sector allow: {ticker} ({sector}, score {score:.0f})")
        elif sector in SECTOR_BLOCK:
            gated_out.append(pick)
            _gate_decisions[ticker] = False
            if verbose:
                print(f"  🚫 sector block: {ticker} ({sector}, score {score:.0f})")
        else:
            # Unknown/neutral sector — ML gate (compression-aware, hysteresis-protected)
            ml_pass = gate.decide(ticker, ml_prob, score=score)
            _gate_decisions[ticker] = ml_pass
            if ml_pass:
                passed.append(pick)
                if verbose:
                    print(f"  🚦 ML gate: {ticker} ({sector or 'UNKNOWN'}, "
                          f"score {score:.0f}, ML {ml_prob:.4f}) → pass")
            else:
                gated_out.append(pick)
                if verbose:
                    print(f"  🚦 ML gate: {ticker} ({sector or 'UNKNOWN'}, "
                          f"score {score:.0f}, ML {ml_prob:.4f}) → blocked")

    gate.save_state(_gate_decisions)

    _substitution_tickers: list = []     # tickers that entered via gate reserve
    if gated_out:
        # Replace gated picks with next-best from screener reserve.
        # Reserve candidates must: not be in basket, not be on cooldown,
        # have no negative news adjustment, and pass the sector-first gate.
        _cd_set, _ = get_cooldown_set(verbose=False)
        reserve = []
        for grp in ["TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3"]:
            for p in screener_picks.get(grp, []):
                _tkr = p.get("ticker")
                if _tkr in basket_tickers:
                    continue
                if _tkr in _pre_gate_excluded:          # blocked by sector≥75 or sector-cap
                    continue
                if _tkr in _cd_set:
                    continue
                if (p.get("news_adjustment", 0) or 0) < 0:
                    continue
                _sec_r = (p.get("sector") or "").strip().upper()
                _scr   = p.get("score", 0) or 0
                _mp_r  = p.get("ml_prob", 0.5) or 0.5
                if _sec_r == "MATERIALS" and _scr >= 75:
                    continue                              # materials≥75 blocked in reserve too
                if _scr >= ML_GATE_SCORE_MIN:
                    if _sec_r in SECTOR_BLOCK:
                        continue                          # sector-blocked in reserve too
                    if _sec_r not in SECTOR_ALLOW and not gate.decide(_tkr, _mp_r, score=_scr):
                        continue                          # ML-gated in reserve too
                reserve.append(p)
                basket_tickers.add(_tkr)
        reserve.sort(key=lambda x: x.get("score", 0), reverse=True)
        print(f"  📋 Reserve pool: {len(reserve)} candidates (post-cap, post-cooldown)")
        replacements = reserve[:len(gated_out)]
        tfsa_picks   = passed + replacements

        gated_tickers = [p.get("ticker") for p in gated_out]
        repl_tickers  = [p.get("ticker") for p in replacements]
        _substitution_tickers = repl_tickers
        print(f"  🔄 Gate removed {gated_tickers}"
              + (f", added {repl_tickers}" if repl_tickers else " (no replacements)"))

    # ── TARGET WEIGHTS (capital-agnostic) ────────────────────────────────────
    target_weights = compute_target_weights(
        tfsa_picks, regime,
        sector_sentiment=sector_sentiment or {},
        win_rate_data=win_rate_data,
        verbose=verbose,
    )

    # ── ACCOUNT ALLOCATIONS ───────────────────────────────────────────────────
    # Load accounts.json; fall back to legacy $10k TFSA when absent.
    accounts = load_accounts()
    account_allocations = {}

    if accounts:
        # Render dollar tables for every account that has capital > 0
        for acct in accounts:
            acct_capital = float(acct.get("capital", 0))
            acct_name    = acct.get("name", "ACCOUNT")
            _acct_def = {**acct, "max_equity": float(acct.get("max_equity", 1.0))}
            alloc = render_allocations(
                target_weights, _acct_def, regime,
                current_drawdown=0.0,
                verbose=(acct_capital > 0 and verbose),
            )
            if alloc:
                account_allocations[acct_name] = alloc

        # Legacy sized list = first account with capital > 0, or empty list
        sized = next(
            (account_allocations[acct.get("name", "")]
             for acct in accounts
             if float(acct.get("capital", 0)) > 0
             and acct.get("name", "") in account_allocations),
            [],
        )
    else:
        # accounts.json absent — legacy $10k TFSA (matches prior output to the cent)
        _legacy_acct = {"name": "TFSA", "capital": 10000,
                        "universe": "ALL", "max_equity": max_equity}
        sized = render_allocations(
            target_weights, _legacy_acct, regime,
            current_drawdown=0.0, verbose=verbose,
        )

    backtest = run_backtest_summary(regime, predictor, verbose=verbose)

    if verbose and predictor.feature_importance:
        print(f"\n🧠 TOP PREDICTIVE FEATURES:")
        for feat, imp in list(predictor.feature_importance.items())[:5]:
            bar = "█" * int(imp * 50)
            print(f"   {feat:<20} {bar} {imp:.3f}")

    return {
        "regime":              regime,
        "ml_trained":          predictor.trained,
        "feature_importance":  predictor.feature_importance,
        "position_sizing":     sized,          # backward compat (dollar amounts)
        "sized_positions":     sized,          # fixes evidence enrichment fallback
        "target_weights":      target_weights, # capital-agnostic weight fractions
        "account_allocations": account_allocations,
        "backtest_summary":    backtest,
        "picks_scored":        len(all_picks),
        "regime_signal":       regime["signal"],
        "gate":                gate,           # MLGate instance — conviction path reads this
        "substitution_tickers": _substitution_tickers,
        "pre_gate_excluded":    sorted(_pre_gate_excluded),   # block75 + sector-cap removals
    }


def calculate_portfolio_metrics(returns_history):
    if not HAS_PANDAS or not returns_history:
        return {}
    rets = pd.Series(returns_history)
    n    = len(rets)
    if n < 3:
        return {"note": "Need more history for metrics"}
    cagr     = ((1 + rets).prod() ** (12 / n) - 1) if n >= 2 else 0
    vol      = rets.std() * (12 ** 0.5)
    rf_m     = 0.05 / 12
    sharpe   = ((rets - rf_m).mean() / rets.std() * (12 ** 0.5)) if rets.std() > 0 else 0
    downside = rets[rets < 0].std() * (12 ** 0.5)
    sortino  = ((rets - rf_m).mean() * 12 / downside) if downside > 0 else 0
    cum      = (1 + rets).cumprod()
    max_dd   = ((cum - cum.expanding().max()) / cum.expanding().max()).min()
    calmar   = (cagr / abs(max_dd)) if max_dd != 0 else 0
    return {
        "cagr_pct": round(cagr*100,2), "volatility_pct": round(vol*100,2),
        "sharpe": round(sharpe,3), "sortino": round(sortino,3),
        "max_drawdown_pct": round(max_dd*100,2), "calmar": round(calmar,3),
        "n_periods": n
    }


if __name__ == "__main__":
    print("ML Engine v2.1 — Kelly sizing + score smoothing")
    regime = get_market_regime(verbose=True)
    p = StockMLPredictor()
    p.train(verbose=True)
    test_f = {"momentum_6m":0.12,"momentum_12m":0.18,"roe":0.22,"profit_margin":0.15,
              "earnings_yield":0.05,"fcf_yield":0.04,"volatility_90d":0.18,"beta":1.1,
              "rev_growth":0.12,"earn_growth":0.15,"div_yield":0.03,"debt_equity":0.4,
              "rs_rating":0.82,"market_regime":1,"sector_momentum":0.05}
    raw = p.predict(test_f, market_regime=1)
    smoothed = smooth_ml_prob("TEST", raw)
    print(f"\nTest prediction: raw={raw:.3f} smoothed={smoothed:.3f}")
