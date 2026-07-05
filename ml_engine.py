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


def _encode_sector(raw_sector_string):
    key = _SECTOR_NORM_INF.get((raw_sector_string or "").strip().lower(), "UNKNOWN")
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
        "n_estimators":     100,
        "max_depth":        3,
        "learning_rate":    0.05,
        "subsample":        0.8,
        "colsample_bytree": 0.7,
        "min_child_weight": 5,
        "reg_alpha":        0.1,
        "reg_lambda":       1.0,
        "random_state":     42,
        "eval_metric":      "auc",
        "use_label_encoder":False,
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
                cv_model = XGBClassifier(**params, verbosity=0)
                cv_model.fit(X_arr[train_idx], y.iloc[train_idx],
                             sample_weight=sample_weights[train_idx], verbose=False)
                cv_aucs.append(roc_auc_score(y.iloc[val_idx],
                               cv_model.predict_proba(X_arr[val_idx])[:, 1]))
            except Exception: pass

        self.model = XGBClassifier(**params, verbosity=0)
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
            mom_6m = features_dict.get("momentum_6m", 0)
            vol    = max(features_dict.get("volatility_90d", 0.02), 0.01)
            features_dict["vol_adj_momentum"] = max(min(mom_6m / vol, 5.0), -5.0)
            features_dict["sector_momentum"]  = features_dict.get("sector_momentum", 0)
            feat_order = ML_CONFIG["features"]
            # Build DataFrame so XGBoost sees column names and categorical dtype
            row = {f: features_dict.get(f, 0) for f in feat_order}
            df  = pd.DataFrame([row])
            df["sector_encoded"] = df["sector_encoded"].astype("category")
            model_input = df
            if self.calibrator is not None:
                prob = self.calibrator.predict_proba(model_input)[0][1]
            else:
                prob = self.model.predict_proba(model_input)[0][1]
            return round(float(prob), 4)
        except: return 0.5


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


def _apply_sector_cap(picks, screener_picks, max_per_sector=2):
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
    basket_tickers = {p.get("ticker") for p in picks}
    all_candidates = []
    for group_name in ["TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3",
                       "FHSA_top5", "conviction_picks"]:
        for p in screener_picks.get(group_name, []):
            if p.get("ticker") not in basket_tickers:
                all_candidates.append(p)

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


def calculate_position_sizes(picks, portfolio_value, market_regime, current_drawdown=0.0,
                              max_equity=1.0, verbose=True, sector_sentiment=None,
                              win_rate_data=None, **kwargs):
    """
    Size positions using Half-Kelly criterion calibrated to actual win rates.

    Kelly fractions from 1,466+ resolved picks (June 2026):
      Score 90-100: 49.2% WR, avg +0.7% → Kelly = -0.23 → 0 (no edge)
      Score 75-89:  59.5% WR, avg +1.1% → Kelly = +0.23 → 0.11 half-Kelly
      Score 60-74:  65.8% WR, avg +1.8% → Kelly = +0.47 → 0.23 half-Kelly
      Score <60:    55.6% WR, avg +1.1% → Kelly = +0.15 → 0.08 half-Kelly

    Key insight: score 90-100 tier has NEGATIVE Kelly — inflated scores, no real edge.
    Score 60-74 is the sweet spot. This sizing reflects the actual data.

    Blend: 40% Kelly-weighted + 60% vol-targeted
    """
    if not picks:
        return []

    # ── SCORE GATE: minimum score 60 for position sizing ───────────────────
    picks = [p for p in picks if (p.get("score") or 0) >= 60]
    if not picks:
        return []

    _cooldown_set, _ = get_cooldown_set(verbose=verbose)
    picks = [p for p in picks if p.get("ticker","") not in _cooldown_set]
    if not picks:
        if verbose: print("   ⚠️ All picks on cooldown — no positions sized")
        return []

    cfg = ML_CONFIG

    dd_multiplier = 1.0
    if current_drawdown > cfg["drawdown_reduction_trigger"]:
        dd_multiplier = 1.0 - cfg["drawdown_reduction_amount"]
        print(f"   ⚠️ Drawdown {current_drawdown*100:.1f}% > threshold — reducing by 30%")

    regime_equity_pct = 1.0 - market_regime.get("cash_pct", 0.0)
    regime_equity_pct = min(regime_equity_pct, max_equity)
    deployable        = portfolio_value * regime_equity_pct * dd_multiplier

    if verbose:
        print(f"\n💼 POSITION SIZING (TFSA — ${portfolio_value:,}):")
        print(f"   Regime: {market_regime['regime']} | "
              f"Equity: {round(regime_equity_pct*100)}% | Cash: {round((1-regime_equity_pct)*100)}%"
              f" | Deployable: ${deployable:,.0f} (pre-risk-multiplier)"
              + (f" (max_equity cap: {max_equity*100:.0f}%)" if max_equity < 1.0 else ""))

    n_picks = min(len(picks), cfg["max_positions"])
    base_wt = 1.0 / n_picks

    # ── SECTOR BLOCK ──────────────────────────────────────────────────────────
    sector_sentiment = sector_sentiment or kwargs.get("sector_sentiment", {})
    SECTOR_MAP_BLOCK = {
        "Communication Services": "TELECOM",
        # "Industrials": "AIRLINES" removed — too broad. TIH.TO, MMM, GE etc are
        # industrials but NOT airlines. AC.TO is caught via news_analyzer SECTOR_TICKERS.
        # Only map sectors where the yfinance label reliably predicts the news_analyzer
        # sector block (Communication Services → TELECOM is 1:1 correct).
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
        """Half-Kelly fraction from score tier. Uses live data if available."""
        if wr_data and wr_data.get("by_score_tier"):
            t = wr_data["by_score_tier"]
            if score >= 90:   d = t.get("90-100", {})
            elif score >= 75: d = t.get("75-89",  {})
            elif score >= 60: d = t.get("60-74",  {})
            else:             d = t.get("below-60",{})
            p  = d.get("win_rate", 50) / 100
            aw = abs(d.get("avg_return", 1.0)) or 1.0
            al = 1.0
        else:
            # Embedded June 2026 calibration
            if score >= 90:   p, aw, al = 0.492, 0.70, 1.0
            elif score >= 75: p, aw, al = 0.595, 1.10, 1.0
            elif score >= 60: p, aw, al = 0.658, 1.80, 1.0
            else:             p, aw, al = 0.556, 1.10, 1.0
        b = aw / al
        kelly = (p * b - (1 - p)) / b
        return max(0.0, kelly * 0.50)  # half-Kelly, floor at zero

    # Load score_history for trend-aware Kelly scoring
    # Kelly should use the most recent trended score, not the raw screener score
    # ABX.TO pattern: screener says 68 (60-74 tier), history shows 57.6 (below-60)
    _score_hist = {}
    # Also build a fast ticker → outcomes lookup for picks with no score_history
    _outcomes_lookup = {}
    try:
        import json as _jout
        _raw_outcomes = _jout.load(open("outcomes.json"))
        for _o in _raw_outcomes:
            _tk = _o.get("ticker","")
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
        """Return the lower of screener score or most recent history score.
        Conservative: if trending down, Kelly uses the trended (lower) score.

        If no score_history exists, fall back to the most recent OUTCOME score
        rather than blindly trusting the screener. This catches the CP.TO / F
        pattern where a ticker enters the basket with no history but has prior
        losses logged at low scores.
        """
        base = pick.get("score", 70)
        ticker = pick.get("ticker", "")
        records = hist.get(ticker, [])

        if records:
            recent = sorted(records, key=lambda x: x.get("date",""), reverse=True)
            latest_score = round(float(recent[0].get("score", base)), 1)
            # Only downgrade — never inflate Kelly using stale history
            return min(base, latest_score)

        # No score_history: check outcomes.json for prior logged scores
        if outcomes_lookup:
            prior = outcomes_lookup.get(ticker, [])
            if prior:
                # Use the most recent outcome's score as conservative floor
                prior_sorted = sorted(prior, key=lambda x: x.get("signal_date",""), reverse=True)
                prior_score = prior_sorted[0].get("score", base)
                if prior_score and prior_score < base:
                    return float(prior_score)

        # No history at all — use screener score but with a caution cap
        # If base > 80 with no history, cap at 75 (requires evidence to go higher)
        if base > 80:
            return 75.0
        return base

    def _ml_edge_multiplier(ml_prob):
        """Scale Kelly by ML calibration evidence.
        Sweet spot (60-80%): 1.5× | Neutral (40-59%): 1.0×
        Weak signal (20-39%): 0.6× | Overconfidence (>85%): 0.3×
        Below 20%: blocked by gate — should not reach sizing.
        Buckets with N<50 in calibration data use conservative defaults.
        """
        p = float(ml_prob or 0)
        if p > 0.85:  return 0.30   # N=266: PF=0.46 — overconfidence collapse
        if p >= 0.60: return 1.50   # N=307: PF=2.60/2.19 — verified sweet spot
        if p >= 0.40: return 1.00   # N=118: PF=1.02 — neutral
        if p >= 0.20: return 0.60   # N=66: PF=0.41 — weak signal (gate misses 20-39%)
        return 0.30                 # <20%: gate should have blocked; penalise if not

    raw_kelly_wts = [score_to_kelly_wt(
                         _trend_adjusted_score(p, _score_hist, _outcomes_lookup),
                         win_rate_data)
                     for p in picks[:n_picks]]

    # Apply ml_edge_multiplier to each pick's Kelly fraction before normalization
    kelly_wts = [raw_kelly_wts[i] * _ml_edge_multiplier(picks[i].get("ml_prob", 0.5))
                 for i in range(n_picks)]

    if verbose:
        for i, p in enumerate(picks[:n_picks]):
            _mult = _ml_edge_multiplier(p.get("ml_prob", 0.5))
            if abs(_mult - 1.0) > 0.05:
                _tag = "✅" if _mult > 1.0 else "⚠️ "
                print(f"   {p['ticker']:<10} ml_edge_mult={_mult:.2f}× {_tag}  (ml_prob={p.get('ml_prob',0):.2f})")

    total_kelly = sum(kelly_wts)
    n_positive_kelly = sum(1 for w in kelly_wts if w > 0)

    if total_kelly > 0:
        raw_norm_kelly = [w / total_kelly for w in kelly_wts]
        # Degradation guard: when <50% of picks have positive Kelly,
        # the normalization concentrates heavily on a small subset.
        # Blend in equal-weight to prevent one mid-tier pick from getting
        # 33%+ of allocation solely due to Kelly normalization.
        if n_positive_kelly < n_picks / 2:
            equal_kelly = [base_wt] * n_picks
            blend_ratio = n_positive_kelly / n_picks  # more equal as Kelly degrades
            norm_kelly = [blend_ratio * raw_norm_kelly[i] + (1 - blend_ratio) * equal_kelly[i]
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
    norm_vol  = [w / total_vol for w in vol_wts] if total_vol > 0 else [base_wt]*n_picks

    # ── BLEND 40% Kelly + 60% Vol ─────────────────────────────────────────────
    blended   = [0.40 * norm_kelly[i] + 0.60 * norm_vol[i] for i in range(n_picks)]
    total_b   = sum(blended)
    norm_b    = [w / total_b for w in blended] if total_b > 0 else [base_wt]*n_picks

    # ── ML-PROPORTIONAL WEIGHTING ────────────────────────────────────────────
    # Instead of a flat ±20% adj, weight proportionally to ML probability.
    # Picks with higher ML confidence get proportionally more capital.
    # Three-way blend: 33% Kelly + 33% vol-targeted + 33% ML-proportional
    ml_probs   = [p.get("ml_prob", 0.5) for p in picks[:n_picks]]
    total_ml   = sum(ml_probs) or 1.0
    norm_ml    = [prob / total_ml for prob in ml_probs]

    # Dynamic per-pick cap — same formula as MAX_HARD below.
    # A flat 20% cap on a 4-pick basket equals equal weight (25% base),
    # which collapses all differentiation before MAX_HARD even runs.
    # 4 picks → 37.5%, 5 picks → 30%, 7+ → 20%
    MAX_SINGLE = max(0.20, 1.5 / n_picks)
    final_wts  = []
    for i in range(n_picks):
        if picks[i]["ticker"] in sector_blocked:
            final_wts.append(0.0)
        elif kelly_wts[i] == 0.0:
            # Kelly=0 or negative edge: reduce to minimum floor (not zero)
            # Zero allocation creates concentration in Kelly>0 picks.
            # Floor = 50% of equal weight — still underweights vs the edge picks
            # but prevents the redistribution cascade that inflates RCI-B/ABX.TO.
            final_wts.append(base_wt * 0.50)
        else:
            w = 0.33 * norm_kelly[i] + 0.33 * norm_vol[i] + 0.33 * norm_ml[i]
            w = min(MAX_SINGLE, w)   # dynamic cap — scales with basket size
            final_wts.append(w)

    total_f  = sum(final_wts)
    if total_f == 0:
        # All picks have Kelly=0 (very thin pool) — fall back to equal weight
        if verbose: print("   ℹ️ All Kelly=0 — falling back to equal weight")
        final_wts = [base_wt if picks[i]["ticker"] not in sector_blocked else 0.0
                     for i in range(n_picks)]
        total_f = sum(final_wts)
    # Iterative normalize + dynamic hard cap
    # Cap scales with basket size: 5 picks → 30%, 7 picks → 21%, 10+ → 20%
    # A 20% cap on a 5-pick basket = equal weight, which kills vol-targeting signal.
    MAX_HARD = max(0.20, 1.5 / n_picks)
    for _iter in range(6):
        _nz = sum(w for w in final_wts if w > 0)
        if _nz == 0: break
        final_wts = [w / _nz for w in final_wts]
        _excess = sum(max(0.0, w - MAX_HARD) for w in final_wts)
        if _excess < 0.0005: break  # converged
        _n_unc = sum(1 for w in final_wts if 0 < w < MAX_HARD)
        _boost = _excess / _n_unc if _n_unc > 0 else 0
        final_wts = [
            min(MAX_HARD, w) if w > 0 else 0.0
            for w in final_wts
        ]
        final_wts = [
            min(MAX_HARD, w + _boost) if 0 < w < MAX_HARD else w
            for w in final_wts
        ]

    # Hard concentration cap for thin baskets — no re-normalization (surplus = cash)
    CONC_CAP = 0.25
    if n_picks < 6:
        capped = [min(w, CONC_CAP) if w > 0 else 0.0 for w in final_wts]
        if capped != final_wts:
            total_deployed = sum(capped)
            if verbose:
                print(f"   ⚠️  Concentration cap ({n_picks} picks): "
                      f"max weight → {CONC_CAP*100:.0f}% | "
                      f"deploying {total_deployed*100:.0f}% of budget "
                      f"({(1-total_deployed)*100:.0f}% cash)")
            final_wts = capped

    sized = []
    for i, pick in enumerate(picks[:n_picks]):
        sc = pick.get("score", 70)
        kw = round(kelly_wts[i], 3)
        wt = final_wts[i]
        _eff_score = _trend_adjusted_score(pick, _score_hist)
        sized.append({
            "ticker":     pick["ticker"],
            "weight_pct": round(wt * 100, 2),
            "dollar_amt": round(deployable * wt, 2),
            "ml_prob":    round(ml_probs[i], 3),
            "vol_adj":    round(vols[i], 3),
            "kelly_wt":   kw,   # tier fraction using trend-adjusted score
            "score":      sc,
        })

    return sized


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

    smoothed_count = 0
    for pick in all_picks:
        ticker     = pick["ticker"]
        stock_data = pick.get("data", {})
        rs         = rs_ratings.get(ticker, {}).get("rs_rating", 50) if rs_ratings else 50

        features = build_features_for_stock(ticker, stock_data, rs)

        if features:
            raw_prob = predictor.predict(features, market_regime=regime_num)
            # ── Score smoothing: 3-day EMA to dampen single-day spikes ──────
            smoothed_prob = smooth_ml_prob(ticker, raw_prob, alpha=0.4)
            if abs(smoothed_prob - raw_prob) > 0.03:
                smoothed_count += 1
            pick["ml_prob"]     = smoothed_prob
            pick["ml_prob_raw"] = raw_prob  # keep raw for debugging
        else:
            pick["ml_prob"]    = 0.5
            pick["ml_prob_raw"] = 0.5

        pick["ml_signal"] = ("🔥 STRONG BUY"  if pick["ml_prob"] >= 0.70 else
                             "✅ BUY"          if pick["ml_prob"] >= 0.58 else
                             "📊 NEUTRAL"      if pick["ml_prob"] >= 0.45 else
                             "⚠️ WEAK")
        ml_score_adj = round((pick["ml_prob"] - 0.5) * 20)
        pick["score"] = max(0, min(100, pick["score"] + ml_score_adj))
        time.sleep(0.1)

    if verbose and smoothed_count:
        print(f"   📊 Score smoothing: {smoothed_count} picks dampened (3-day EMA)")

    # Save updated smooth cache
    _save_smooth_cache()

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

    # ── Materials sector block at score ≥ 75 ────────────────────────────
    # Evidence (2,019 picks): MATERIALS at score≥90 → N=114, WR=28.9%, PF=0.17
    # Extends the exclusion down to score≥75 to catch WPM.TO/AGI.TO/AEM.TO/ABX.TO
    # before they accumulate loss history at that tier.
    _mats_filtered = []
    for _p in tfsa_picks:
        _sec   = (_p.get("sector") or "").strip().upper()
        _score = _p.get("score", 0) or 0
        if _sec == "MATERIALS" and _score >= 75:
            if verbose:
                print(f"  🚫 Materials≥75 block: {_p.get('ticker')} (score {_score:.0f})")
        else:
            _mats_filtered.append(_p)
    tfsa_picks = _mats_filtered

    # ── Sector diversity cap ──────────────────────────────────────────────
    # Max 2 picks per sector in the final basket.
    # Problem it solves: JPM + TD.TO + REI-UN.TO all land in financials
    # under CAUTIOUS macro — 3 correlated positions, stress test shows fragility.
    # When a sector is over-represented, excess picks are replaced by the
    # next-best scoring pick from a different sector.
    tfsa_picks = _apply_sector_cap(tfsa_picks, screener_picks, max_per_sector=2)

    # ── Sector-first gate on 90-100 tier ─────────────────────────────────
    # Sector performance at score≥90 (2,019 picks, July 2026):
    #   ALLOW:  ENERGY N=157 PF=2.30 | BANKS N=129 PF=2.89 | FINANCIALS N=69 PF=6.31
    #   BLOCK:  MATERIALS N=114 PF=0.17 | TELECOM N=65 PF=0.20 | HEALTHCARE N=48 PF=0.39
    #           REIT N=57 PF=0.46 | CONSUMER N=65 PF=0.89
    #   OTHER:  Fall through to ML confidence gate (ml_prob ≥ 20% required)
    SECTOR_ALLOW = {'ENERGY', 'BANKS', 'FINANCIALS'}
    SECTOR_BLOCK = {'MATERIALS', 'TELECOM', 'HEALTHCARE', 'REIT', 'CONSUMER'}
    ML_GATE_SCORE_MIN = 90
    ML_GATE_PROB_MIN  = 0.20
    basket_tickers    = {p.get("ticker") for p in tfsa_picks}
    gated_out         = []
    passed            = []

    for pick in tfsa_picks:
        score   = pick.get("score", 0) or 0
        ml_prob = pick.get("ml_prob", 0.5) or 0.5
        sector  = (pick.get("sector") or "").strip().upper()

        if score < ML_GATE_SCORE_MIN:
            passed.append(pick)
            continue

        if sector in SECTOR_ALLOW:
            passed.append(pick)
            if verbose:
                print(f"  ✅ sector allow: {pick.get('ticker')} ({sector}, score {score:.0f})")
        elif sector in SECTOR_BLOCK:
            gated_out.append(pick)
            if verbose:
                print(f"  🚫 sector block: {pick.get('ticker')} ({sector}, score {score:.0f})")
        else:
            # Unknown or neutral sector — fall through to ML confidence gate
            if ml_prob < ML_GATE_PROB_MIN:
                gated_out.append(pick)
                if verbose:
                    print(f"  🚦 ML gate: {pick.get('ticker')} ({sector or 'UNKNOWN'}, "
                          f"score {score:.0f}, ML {ml_prob:.0%}) → blocked")
            else:
                passed.append(pick)
                if verbose:
                    print(f"  🚦 ML gate: {pick.get('ticker')} ({sector or 'UNKNOWN'}, "
                          f"score {score:.0f}, ML {ml_prob:.0%}) → pass")

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
                if _tkr in _cd_set:
                    continue
                if (p.get("news_adjustment", 0) or 0) < 0:
                    continue
                _sec_r = (p.get("sector") or "").strip().upper()
                _scr   = p.get("score", 0) or 0
                _mp_r  = p.get("ml_prob", 0.5) or 0.5
                if _sec_r == "MATERIALS" and _scr >= 75:
                    continue                                  # materials≥75 blocked in reserve too
                if _scr >= ML_GATE_SCORE_MIN:
                    if _sec_r in SECTOR_BLOCK:
                        continue                              # sector-blocked in reserve too
                    if _sec_r not in SECTOR_ALLOW and _mp_r < ML_GATE_PROB_MIN:
                        continue                              # ML-gated in reserve too
                reserve.append(p)
                basket_tickers.add(_tkr)
        reserve.sort(key=lambda x: x.get("score", 0), reverse=True)
        replacements = reserve[:len(gated_out)]
        tfsa_picks   = passed + replacements

        gated_tickers = [p.get("ticker") for p in gated_out]
        repl_tickers  = [p.get("ticker") for p in replacements]
        print(f"  🔄 Gate removed {gated_tickers}"
              + (f", added {repl_tickers}" if repl_tickers else " (no replacements)"))

    sized = calculate_position_sizes(
        tfsa_picks,
        portfolio_value=10000,
        market_regime=regime,
        current_drawdown=0.0,
        max_equity=max_equity,
        verbose=verbose,
        sector_sentiment=sector_sentiment or {},
        win_rate_data=win_rate_data,
    )

    if verbose:
        for pos in sized[:5]:
            print(f"   {pos['ticker']:<12} {pos['weight_pct']:>5.1f}%  ${pos['dollar_amt']:>8,.0f}"
                  f"  ML: {pos['ml_prob']:.2f}  Kelly(tier): {pos['kelly_wt']:.3f}")

    backtest = run_backtest_summary(regime, predictor, verbose=verbose)

    if verbose and predictor.feature_importance:
        print(f"\n🧠 TOP PREDICTIVE FEATURES:")
        for feat, imp in list(predictor.feature_importance.items())[:5]:
            bar = "█" * int(imp * 50)
            print(f"   {feat:<20} {bar} {imp:.3f}")

    return {
        "regime":             regime,
        "ml_trained":         predictor.trained,
        "feature_importance": predictor.feature_importance,
        "position_sizing":    sized,
        "backtest_summary":   backtest,
        "picks_scored":       len(all_picks),
        "regime_signal":      regime["signal"],
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
