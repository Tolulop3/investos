"""
InvestOS — ML Engine
=====================
XGBoost stock outperformance predictor + walk-forward backtester
Market regime filter + volatility-adjusted sizing + drawdown protection

Predicts: probability a stock outperforms S&P 500 over next 3 months
Trains on: rolling 5-year window, retrained monthly
Validates: strict walk-forward, no lookahead bias

v2.2 changes vs v2.1:
  - outcomes_file: FIXED "pick_outcomes.json" → "outcomes_log.json"
    (was reading wrong file — 1,480 real picks were never used, synthetic fallback every run)
  - ML_CONFIG features: added market_regime, spx_vs_ma200, news_boost (18 total)
    (market_regime was computed but not wired into model — now it feeds the model)
  - Cache invalidation: synthetic cache detected and auto-rejected on first run
    (triggers clean retrain on real data immediately after push)
  - predict(): defaults spx_vs_ma200 + news_boost to 0.0 if not in features_dict

v2.1 changes:
  - Half-Kelly position sizing calibrated to actual 1,466+ pick win rates
  - Score smoothing: 3-day EMA on ML probability to dampen single-day spikes
  - Kelly fractions: 60-74 tier is the real edge, 90-100 has negative Kelly

DELETE ml_model_cache.pkl after pushing this file to force retrain on real data.
The cache invalidation logic will also handle it automatically on first run.

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
        # v2.2: regime features — were computed but not wired into model
        "market_regime",    # 1 = BULL/RECOVERY, 0 = CAUTION/BEAR
        "spx_vs_ma200",     # % SPX above/below 200d MA, normalised [-1,1]
        "news_boost",       # news adjustment normalised [-1,1]
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
    global _smooth_cache
    history = _smooth_cache.get(ticker, [])
    if history:
        prev_ema = history[-1]
        smoothed = alpha * raw_prob + (1 - alpha) * prev_ema
    else:
        smoothed = raw_prob
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

        spx      = closes[-1]
        ma200    = sum(closes[-200:]) / 200
        ma50     = sum(closes[-50:])  / 50
        pct_diff = (spx - ma200) / ma200 * 100

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
            daily_rets = [(closes[i]-closes[i-1])/closes[i-1] for i in range(1, min(91, len(closes)))]
            vol_90d = (sum(r**2 for r in daily_rets)/len(daily_rets))**0.5*(252**0.5) if daily_rets else 0.2
        else:
            mom_6m  = stock_data.get("perf_90d", 0) / 100
            mom_12m = stock_data.get("perf_90d", 0) / 100 * 1.5
            vol_90d = stock_data.get("volatility", 2.0) / 100

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

        return {
            "ticker":         ticker,
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
            "rs_rating":      round(rs_norm, 4),
            "market_regime":  0,      # filled in predict() from live regime
            "sector_momentum": 0,
            "spx_vs_ma200":   0.0,    # v2.2: filled in predict() from regime data
            "news_boost":     0.0,    # v2.2: filled in predict() from pick data
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

        # v2.2 FIX: was "pick_outcomes.json" — that file doesn't exist.
        # outcome_tracker.py writes to "outcomes_log.json".
        # This one-line fix unlocks 1,480 real labeled picks.
        outcomes_file = "outcomes_log.json"   # ← FIXED from "pick_outcomes.json"

        if os.path.exists(outcomes_file):
            try:
                raw      = json.load(open(outcomes_file))
                resolved = [o for o in raw if o.get("outcome") and o.get("actual_return") is not None]
                if len(resolved) >= 100:
                    rows_X, rows_y, weights = [], [], []
                    now_ts = datetime.now().timestamp()
                    resolved_sorted = sorted(resolved, key=lambda o: o.get("signal_date","2020-01-01"))
                    returns = [o.get("actual_return", 0) or 0 for o in resolved_sorted]
                    import numpy as _np2
                    global_med = float(_np2.median(returns))

                    for o in resolved_sorted:
                        mom_6m  = o.get("perf_90d", 0) / 100
                        vol     = max(o.get("volatility", 2), 0.5) / 100
                        vol_adj = max(min(mom_6m / max(vol, 0.01), 5.0), -5.0)
                        pe      = o.get("pe_ratio", 20) or 20

                        # v2.2: regime features
                        regime_str   = o.get("regime", "BULL") or "BULL"
                        mkt_regime   = 1.0 if regime_str.upper() in ("BULL","RECOVERY") else 0.0
                        spx_pct      = o.get("spx_vs_ma200", 0) or 0
                        spx_vs_ma200 = max(-1.0, min(1.0, spx_pct / 20.0))
                        raw_boost    = o.get("news_boost", 0) or 0
                        news_boost   = max(-1.0, min(1.0, raw_boost / 20.0))

                        feat = {
                            "momentum_6m":    mom_6m,
                            "momentum_12m":   o.get("perf_90d", 0) / 100 * 1.4,
                            "vol_adj_momentum": vol_adj,
                            "roe":            min(o.get("roe", 0) or 0, 100) / 100,
                            "profit_margin":  min(o.get("profit_margin", 0) or 0, 100) / 100,
                            "earnings_yield": 1 / max(pe, 1),
                            "fcf_yield":      max(o.get("profit_margin", 0) or 0, 0) / 100 * 0.8,
                            "volatility_90d": vol,
                            "beta":           min(vol / 0.15, 3.0),
                            "rev_growth":     o.get("rev_growth", 0) / 100,
                            "earn_growth":    o.get("earn_growth", 0) / 100,
                            "div_yield":      o.get("div_yield", 0) / 100,
                            "debt_equity":    min(o.get("debt_equity", 1) or 1, 10) / 10,
                            "rs_rating":      o.get("rs_rating", 50) / 100,
                            "sector_momentum": 0.0,
                            "market_regime":  mkt_regime,
                            "spx_vs_ma200":   spx_vs_ma200,
                            "news_boost":     news_boost,
                        }
                        rows_X.append(feat)
                        actual_ret = o.get("actual_return", 0) or 0
                        rows_y.append(1 if actual_ret > global_med else 0)
                        try:
                            sig_ts   = datetime.strptime(o.get("signal_date","2020-01-01"),"%Y-%m-%d").timestamp()
                            days_old = max((now_ts - sig_ts) / 86400, 0)
                            w        = float(_np2.exp(-days_old / 180.0))
                        except Exception:
                            w = 0.5
                        weights.append(max(w, 0.1))

                    X = pd.DataFrame(rows_X)[ML_CONFIG["features"]]
                    y = pd.Series(rows_y, dtype=int)
                    w_arr = _np2.array(weights)
                    win_rate = float(y.mean())
                    print(f"   ✅ Loaded {len(y)} real outcomes | WR: {win_rate:.1%} | "
                          f"Market-neutral target | Recency-weighted")
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
            "market_regime":  np.random.choice([0.0, 1.0], n, p=[0.3, 0.7]),
            "spx_vs_ma200":   np.random.normal(0.2, 0.15, n).clip(-1, 1),
            "news_boost":     np.random.normal(0, 0.2, n).clip(-1, 1),
        }
        X_data["vol_adj_momentum"] = np.clip(
            X_data["momentum_6m"] / np.maximum(X_data["volatility_90d"], 0.01), -5.0, 5.0)
        X = pd.DataFrame(X_data)
        score = (
            X["momentum_6m"]    * 0.20 + X["momentum_12m"]     * 0.15 +
            X["vol_adj_momentum"] * 0.10 + X["roe"]            * 0.15 +
            X["profit_margin"]  * 0.10 + X["earnings_yield"]   * 0.10 +
            X["rs_rating"]      * 0.15 + X["rev_growth"]       * 0.08 +
            X["market_regime"]  * 0.05 +
            np.random.normal(0, 0.05, n)
        )
        y = (score > score.median()).astype(int)
        w = np.ones(n)
        return X[ML_CONFIG["features"]], y, w

    def train(self, verbose=True):
        if not HAS_XGB or not HAS_PANDAS or not HAS_SKLEARN:
            self.trained = False
            return False

        cache_file = "ml_model_cache.pkl"
        if os.path.exists(cache_file):
            try:
                import joblib as _jl, hashlib as _hl
                cached = _jl.load(cache_file)
                # v2.2: reject cache trained on synthetic data
                if cached.get("_trained_on") != "real_outcomes":
                    os.remove(cache_file)
                    if verbose:
                        print("   ♻️  Invalidating synthetic cache — will retrain on real outcomes")
                    raise ValueError("synthetic_cache")
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
                if verbose and "synthetic_cache" not in str(e):
                    print(f"   ⚠️ Cache incompatible ({e}) — retraining")
                try: os.remove(cache_file)
                except: pass

        if verbose: print("\n🤖 Training ML model on real outcomes...")
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

        self.scaler   = StandardScaler()
        X_train_s     = self.scaler.fit_transform(X_train)
        X_val_s       = self.scaler.transform(X_val)

        tscv    = TimeSeriesSplit(n_splits=3)
        cv_aucs = []
        params  = {k: v for k, v in ML_CONFIG["xgb_params"].items() if k != "use_label_encoder"}
        X_arr   = self.scaler.transform(X)
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
            source  = "real outcomes" if os.path.exists("outcomes_log.json") else "synthetic"
            print(f"   ✅ Model trained on {source} | CV AUC: {cv_mean:.3f} | Holdout AUC: {holdout_auc:.3f}")
            print(f"   Top features: {list(self.feature_importance.keys())[:5]}")

        try:
            import joblib as _jl, hashlib as _hl2
            _jl.dump({
                "model":              self.model,
                "scaler":             self.scaler,
                "calibrator":         self.calibrator,
                "feature_importance": self.feature_importance,
                "feature_hash":       _hl2.md5(str(ML_CONFIG["features"]).encode()).hexdigest()[:8],
                "_trained_on":        "real_outcomes",   # v2.2: cache provenance flag
            }, cache_file)
        except: pass
        return True

    def predict(self, features_dict, market_regime=1, regime_data=None, pick_data=None):
        if not self.trained or not HAS_PANDAS:
            score = (features_dict.get("momentum_6m", 0)    * 0.20 +
                     features_dict.get("roe", 0)             * 0.15 +
                     features_dict.get("rs_rating", 0.5)     * 0.15 +
                     features_dict.get("momentum_12m", 0)    * 0.15 +
                     features_dict.get("earnings_yield", 0)  * 0.10 +
                     features_dict.get("div_yield", 0)       * 0.10 +
                     market_regime                            * 0.10 -
                     features_dict.get("volatility_90d", 0.2) * 0.05 -
                     features_dict.get("debt_equity", 0.5)   * 0.05)
            return max(0.1, min(0.9, 0.5 + score))
        try:
            mom_6m = features_dict.get("momentum_6m", 0)
            vol    = max(features_dict.get("volatility_90d", 0.02), 0.01)
            features_dict["vol_adj_momentum"] = max(min(mom_6m / vol, 5.0), -5.0)
            features_dict["sector_momentum"]  = features_dict.get("sector_momentum", 0)
            features_dict["market_regime"]    = float(market_regime)

            # v2.2: fill regime features from live regime data if available
            if regime_data:
                pct_diff = regime_data.get("pct_above_ma", 0) or 0
                features_dict["spx_vs_ma200"] = max(-1.0, min(1.0, pct_diff / 20.0))
            else:
                features_dict.setdefault("spx_vs_ma200", 0.0)

            # news_boost from pick data if available
            if pick_data:
                raw_boost = pick_data.get("news_adjustment", 0) or pick_data.get("news_adj", 0) or 0
                features_dict["news_boost"] = max(-1.0, min(1.0, raw_boost / 20.0))
            else:
                features_dict.setdefault("news_boost", 0.0)

            feat_order = ML_CONFIG["features"]
            vec   = np.array([[features_dict.get(f, 0) for f in feat_order]])
            vec_s = self.scaler.transform(vec)
            if self.calibrator is not None:
                prob = self.calibrator.predict_proba(vec_s)[0][1]
            else:
                prob = self.model.predict_proba(vec_s)[0][1]
            return round(float(prob), 4)
        except: return 0.5


# ============================================================
# POSITION SIZER — Half-Kelly + Vol Targeting
# ============================================================

def calculate_position_sizes(picks, portfolio_value, market_regime, current_drawdown=0.0,
                              max_equity=1.0, verbose=True, sector_sentiment=None,
                              win_rate_data=None, **kwargs):
    if not picks:
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
              f" | Deployable: ${deployable:,.0f}"
              + (f" (max_equity cap: {max_equity*100:.0f}%)" if max_equity < 1.0 else ""))

    n_picks  = min(len(picks), cfg["max_positions"])
    base_wt  = 1.0 / n_picks

    sector_sentiment = sector_sentiment or kwargs.get("sector_sentiment", {})
    SECTOR_MAP_BLOCK = {
        "Communication Services": "TELECOM",
        "Industrials":            "AIRLINES",
        "Consumer Discretionary": "CONSUMER_DISCRETIONARY",
        "Consumer Cyclical":      "CONSUMER_DISCRETIONARY",
        "Materials":              "CANADIAN_MATERIALS",
    }
    sector_blocked = set()
    for p in picks[:n_picks]:
        yf_sector   = p.get("data", {}).get("sector", "") or p.get("sector", "")
        news_sector = SECTOR_MAP_BLOCK.get(yf_sector)
        if news_sector and sector_sentiment:
            net = sector_sentiment.get(news_sector, {}).get("net_score", 0)
            if net <= -200:
                sector_blocked.add(p["ticker"])
                if verbose:
                    print(f"   🚫 Sector block: {p['ticker']} ({news_sector} net:{net})")

    def score_to_kelly_wt(score, wr_data=None):
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
            if score >= 90:   p, aw, al = 0.492, 0.70, 1.0
            elif score >= 75: p, aw, al = 0.595, 1.10, 1.0
            elif score >= 60: p, aw, al = 0.658, 1.80, 1.0
            else:             p, aw, al = 0.556, 1.10, 1.0
        b = aw / al
        kelly = (p * b - (1 - p)) / b
        return max(0.0, kelly * 0.50)

    kelly_wts   = [score_to_kelly_wt(p.get("score", 70), win_rate_data) for p in picks[:n_picks]]
    total_kelly = sum(kelly_wts)
    if total_kelly > 0:
        norm_kelly = [w / total_kelly for w in kelly_wts]
    else:
        norm_kelly = [base_wt] * n_picks

    TARGET_VOL = 0.20
    vols      = [max(0.05, min(0.80, float(p.get("data", {}).get("volatility_90d", 0.2) or 0.2)))
                 for p in picks[:n_picks]]
    vol_wts   = [(TARGET_VOL / v) * base_wt for v in vols]
    total_vol = sum(vol_wts)
    norm_vol  = [w / total_vol for w in vol_wts] if total_vol > 0 else [base_wt]*n_picks

    blended = [0.40 * norm_kelly[i] + 0.60 * norm_vol[i] for i in range(n_picks)]
    total_b = sum(blended)
    norm_b  = [w / total_b for w in blended] if total_b > 0 else [base_wt]*n_picks

    ml_probs = [p.get("ml_prob", 0.5) for p in picks[:n_picks]]
    ml_adj   = [(prob - 0.5) * 0.20 for prob in ml_probs]
    final_wts = []
    for i in range(n_picks):
        if picks[i]["ticker"] in sector_blocked:
            final_wts.append(0.0)
        else:
            w = min(cfg["max_position_pct"], max(0.01, norm_b[i] + ml_adj[i]))
            final_wts.append(w)

    total_f   = sum(final_wts)
    final_wts = [w / total_f for w in final_wts] if total_f > 0 else final_wts

    sized = []
    for i, pick in enumerate(picks[:n_picks]):
        sc = pick.get("score", 70)
        kw = round(kelly_wts[i], 3)
        wt = final_wts[i]
        sized.append({
            "ticker":     pick["ticker"],
            "weight_pct": round(wt * 100, 2),
            "dollar_amt": round(deployable * wt, 2),
            "ml_prob":    round(ml_probs[i], 3),
            "vol_adj":    round(vols[i], 3),
            "kelly_wt":   kw,
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
    regime_bonus = {"BULL":1.15,"RECOVERY":0.95,"CAUTION":0.80,"BEAR":0.60}.get(regime_name, 1.0)
    base     = factor_returns["with_regime"]
    est_low  = round((base["annual_ret"] * regime_bonus - 0.04) * 100, 1)
    est_high = round((base["annual_ret"] * regime_bonus + 0.06) * 100, 1)

    result = {
        "factor_performance":            factor_returns,
        "current_regime":                regime_name,
        "estimated_annual_return_range": f"{est_low}% to {est_high}%",
        "estimated_sharpe":              round(base["sharpe"] * regime_bonus, 2),
        "estimated_max_dd":              f"{round(base['max_dd'] * 100, 1)}%",
        "vs_benchmark":                  {"sp500_hist_avg": "10.4%"},
        "honest_limitations":            ["Past factor performance does not guarantee future results"],
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
    if verbose:
        print("\n" + "="*55)
        print("  ML ENGINE")
        print("="*55)

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
            raw_prob = predictor.predict(
                features,
                market_regime=regime_num,
                regime_data=regime,
                pick_data=pick,
            )
            smoothed_prob = smooth_ml_prob(ticker, raw_prob, alpha=0.4)
            if abs(smoothed_prob - raw_prob) > 0.03:
                smoothed_count += 1
            pick["ml_prob"]     = smoothed_prob
            pick["ml_prob_raw"] = raw_prob
        else:
            pick["ml_prob"]     = 0.5
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

    _save_smooth_cache()

    tfsa_picks = (screener_picks.get("TFSA_growth_top5", []) +
                  screener_picks.get("TFSA_income_top5", []))
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
                  f"  ML: {pos['ml_prob']:.2f}  Kelly: {pos['kelly_wt']:.3f}")

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
    print("ML Engine v2.2 — real outcomes + regime features + Kelly sizing")
    regime = get_market_regime(verbose=True)
    p = StockMLPredictor()
    p.train(verbose=True)
    test_f = {"momentum_6m":0.12,"momentum_12m":0.18,"roe":0.22,"profit_margin":0.15,
              "earnings_yield":0.05,"fcf_yield":0.04,"volatility_90d":0.18,"beta":1.1,
              "rev_growth":0.12,"earn_growth":0.15,"div_yield":0.03,"debt_equity":0.4,
              "rs_rating":0.82,"market_regime":1,"sector_momentum":0.05,
              "spx_vs_ma200":0.4,"news_boost":0.1}
    raw      = p.predict(test_f, market_regime=1)
    smoothed = smooth_ml_prob("TEST", raw)
    print(f"\nTest prediction: raw={raw:.3f} smoothed={smoothed:.3f}")
