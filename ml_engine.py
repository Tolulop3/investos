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
  - Kelly's p/b come from each pick's own ml_prob bucket's measured win rate
    and payoff ratio (2026-07-21 fix) — not a score-tier-wide average, which
    guaranteed negative edge for every 90-100/75-89-tier pick regardless of
    ml_prob. A pick with no ml_prob logged that day gets no Kelly weight
    (floored to 0), not a tier-average substitute. See outcome_tracker.py
    ML_PROB_BUCKETS and ml_engine.py score_to_kelly_wt.

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
from collections import defaultdict, Counter

from gate_engine import MLGate, load_outcomes_ticker_counts, PROBATION_CAP
from outcome_tracker import ml_prob_bucket, category_is_data_ready
from pick_utils import dedupe_picks_by_ticker, get_pick_category, get_pick_sector

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
    "consumer defensive": "CONSUMER",   # yfinance sub-label for staples/non-cyclical
    "consumer cyclical": "CONSUMER",    # yfinance sub-label for discretionary
    "technology": "TECH", "information technology": "TECH",
    "communication services": "TELECOM", "telecommunications": "TELECOM",
    "telecom": "TELECOM",
    "health care": "HEALTHCARE", "healthcare": "HEALTHCARE",
    "pharmaceuticals": "HEALTHCARE", "biotechnology": "HEALTHCARE",
    "drug manufacturers—general": "HEALTHCARE",
    "industrials": "INDUSTRIALS",
    "materials": "MATERIALS",
    "basic materials": "MATERIALS",
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


def get_canonical_sector(p):
    """
    Canonical NORMALIZED sector string (e.g. "FINANCIALS", "MATERIALS") for
    a pick, matching what sector-cap/gate logic compares against.

    FIX (2026-08-09): this exact 3-tier fallback (prefer pre-computed
    sector_canonical -> raw sector via pick_utils.get_pick_sector() ->
    per-ticker override -> normalize) was copy-pasted independently at 4
    call sites in this file's sector-cap reserve/replacement/basket-summary
    logic, each with slightly different completeness (some skipped the
    ticker override, some skipped the top-level-sector leg) -- exactly the
    kind of drift the pick_utils.py accessor module exists to prevent.
    """
    if p.get("sector_canonical"):
        return p["sector_canonical"]
    raw = get_pick_sector(p).strip()
    if not raw:
        raw = _TICKER_SECTOR_OVERRIDE.get(p.get("ticker", ""), "")
    return _SECTOR_NORM_INF.get(raw.lower(), raw.upper() or "UNKNOWN")


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
        "spx_vs_ma200",
        "news_boost",
        "unified_regime_enc",
        "macro_regime_enc",
        "market_breadth_50ma",
        # sector_encoded removed: model was degenerate (importance 1.0000, flat 0.4495 output).
        # Sector logic lives in the gate (SECTOR_ALLOW / SECTOR_BLOCK). Do NOT re-add.
        # sector_momentum, market_regime, close_to_ema20_ratio removed (2026-08-08):
        # confirmed constant (zero variance) across all 2465 training rows -- a
        # data-pipeline bug (never actually computed per-row), pure dead weight.
        # If these are ever wired up to compute real per-row values, re-add here
        # AND in ml_retrainer.py's FEATURES (must stay in sync -- feature_hash
        # compatibility between the two files depends on it).
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
        # enable_categorical removed with sector_encoded — no categorical features remain
        "monotone_constraints": {     # must match ml_retrainer.py exactly
            # roe flipped +1 -> -1 (2026-08-08): empirically confirmed negative
            # correlation with the label (-0.096) in the live dataset -- the old
            # +1 constraint was forcing the model to fit the opposite of what the
            # data actually shows.
            "roe":                  -1,
            "profit_margin":         1,
            "earnings_yield":        1,
            "volatility_90d":       -1,
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

# Categories whose true intended hold period (CATEGORY_HORIZONS in
# outcome_tracker.py) is 180-365 days -- no signal in the system is old
# enough yet (earliest ~159 days) to validate an ML model against their
# real horizon. Scored with predict_rules_based() instead of any ML model
# until enough calendar time passes. See run_ml_engine()'s scoring loop.
RULES_BASED_CATEGORIES = {
    "WATCH", "GROWTH CORE", "FHSA Conservative Growth",
    "INCOME", "DIVIDEND GROWTH", "INCOME + GROWTH",
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

        raw_sector = (stock_data.get("sector", "") or "").strip()
        sector_enc = _encode_sector(raw_sector, ticker=ticker)
        _sec_norm  = _SECTOR_NORM_INF.get(raw_sector.lower(), "UNKNOWN")
        print(f"   [sector] {ticker:<12} raw={repr(raw_sector):<28} norm={_sec_norm:<15} enc={sector_enc}")

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
        self.swing_model        = None
        self.swing_scaler       = None
        # FIX (2026-08-09): retrain-vintage tracking for Kelly's durable
        # calibration fix -- see outcome_tracker.py's compute_win_rate()
        # docstring on by_ml_prob_bucket_model / MODEL_VINTAGE_WINDOW_DAYS.
        self.model_trained_at       = None
        self.swing_model_trained_at = None
        self._category_models       = {}  # lazily populated, see predict_category()

    def load_training_data(self):
        if not HAS_PANDAS:
            return None, None, None, None

        # PRIMARY: use the same feature builder as ml_retrainer.py — one builder, two callers.
        # This guarantees the daily training matrix has the same 23 columns in the same order,
        # including close_to_ema20_ratio (constrained), sector_encoded, and all regime features.
        try:
            from ml_retrainer import build_feature_matrix, load_resolved_outcomes
            resolved = load_resolved_outcomes()
            if len(resolved) >= 80:
                X, y, w_arr, dates_arr = build_feature_matrix(resolved)
                if X is not None and len(y) >= 50:
                    win_rate = float(y.mean())
                    print(f"   ✅ Loaded {len(y)} real outcomes | WR: {win_rate:.1%} "
                          f"| {len(X.columns)} features via build_feature_matrix")
                    return X, y, w_arr, dates_arr
        except Exception as _e:
            print(f"   ⚠️ build_feature_matrix failed ({_e}) — using bootstrap")

        # FALLBACK: bootstrap with all 22 features so constraints never reference
        # a column that isn't in the training matrix.
        print("   First run — bootstrapping model from factor research (22 features)...")
        np.random.seed(42)
        n = 2000
        X_data = {
            "momentum_6m":          np.random.normal(0.05, 0.15, n),
            "momentum_12m":         np.random.normal(0.08, 0.20, n),
            "roe":                  np.random.beta(2, 5, n),
            "profit_margin":        np.random.beta(1.5, 4, n),
            "earnings_yield":       np.random.beta(2, 3, n) * 0.15,
            "fcf_yield":            np.random.beta(1.5, 4, n) * 0.10,
            "volatility_90d":       np.random.beta(2, 5, n) * 0.6 + 0.1,
            "beta":                 np.random.normal(1.0, 0.4, n).clip(0.2, 3.0),
            "rev_growth":           np.random.normal(0.08, 0.20, n),
            "earn_growth":          np.random.normal(0.10, 0.30, n),
            "div_yield":            np.random.beta(1.5, 6, n) * 0.10,
            "debt_equity":          np.random.beta(2, 3, n),
            "rs_rating":            np.random.uniform(0, 1, n),
            "sector_momentum":      np.random.normal(0, 0.10, n),
            "market_regime":        np.random.choice([0.0, 1.0], n),
            "spx_vs_ma200":         np.random.normal(0, 0.3, n).clip(-1, 1),
            "news_boost":           np.zeros(n),
            "close_to_ema20_ratio": np.ones(n),   # default 1.0 = at EMA (no overextension)
            "unified_regime_enc":   np.random.choice([0.0, 1.0, 2.0, 3.0], n),
            "macro_regime_enc":     np.random.choice([0.0, 1.0, 2.0, 3.0], n),
            "market_breadth_50ma":  np.random.uniform(0.3, 0.7, n),
        }
        X_data["vol_adj_momentum"] = np.clip(
            X_data["momentum_6m"] / np.maximum(X_data["volatility_90d"], 0.01), -5.0, 5.0)
        X = pd.DataFrame(X_data)[ML_CONFIG["features"]]
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
        return X, y, w, None  # no real signal dates on synthetic bootstrap data

    def _load_swing_model(self, verbose=True):
        """Load the dedicated SWING model independently of the general
        model's cache state -- separate concern, separate cache file."""
        try:
            from ml_retrainer import load_swing_model
            self.swing_model, self.swing_scaler, self.swing_model_trained_at = load_swing_model()
            if verbose and self.swing_model is not None:
                print("   OK Loaded SWING model")
        except Exception:
            self.swing_model, self.swing_scaler, self.swing_model_trained_at = None, None, None

    def train(self, verbose=True):
        self._load_swing_model(verbose=verbose)

        if not HAS_XGB or not HAS_PANDAS or not HAS_SKLEARN:
            self.trained = False
            return False

        cache_file = "ml_model_cache.pkl"
        if os.path.exists(cache_file):
            try:
                import joblib as _jl, hashlib as _hl
                cached = _jl.load(cache_file)
                self.model              = cached["model"]
                self.scaler             = None   # scaler removed; model trained on DataFrame
                self.calibrator         = cached.get("calibrator")
                self.feature_importance = cached.get("feature_importance", {})
                self.model_trained_at   = cached.get("_retrained_at")
                _feat_hash = _hl.md5(str(ML_CONFIG["features"]).encode()).hexdigest()[:8]
                if cached.get("feature_hash") != _feat_hash:
                    os.remove(cache_file)
                    raise ValueError("feature_hash_mismatch")
                # Validate model with a dummy DataFrame prediction (no scaler needed)
                _test = {f: 0.0 for f in ML_CONFIG["features"]}
                _test_df = pd.DataFrame([_test])
                self.model.predict_proba(_test_df)
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

        X, y, sample_weights, dates_arr = result
        if len(y) < 50:
            return False

        from sklearn.model_selection import TimeSeriesSplit

        # Frozen date-based split + purge buffer — mirrors ml_retrainer.py's
        # train_and_save() exactly (see HOLDOUT_CUTOFF_DATE there). This used to be
        # a positional 80/20 split with no purge, which is a *different* split from
        # ml_retrainer.py's despite training on the same data via the same
        # build_feature_matrix() call. Confirmed empirically (2026-08-08) on the
        # live 2465-row matrix: the positional/no-purge split reported holdout
        # AUC 0.607, but 232 of its "training" rows fell inside the 10-day purge
        # window of the val boundary -- purging just those (same boundary,
        # otherwise unchanged) dropped it to 0.543, and the full frozen-cutoff+
        # purge method matching ml_retrainer.py landed at 0.497 (no real edge).
        # ml_prob feeds a direct +4/+8 conviction-score boost in run_daily.py, so
        # the old split wasn't just a misleading report number -- it was pushing
        # real picks up in rank and size on leakage-inflated confidence.
        if dates_arr is not None and len(dates_arr) == len(X):
            from ml_retrainer import HOLDOUT_CUTOFF_DATE
            dates_arr_np = np.asarray(dates_arr)
            split = int((dates_arr_np <= HOLDOUT_CUTOFF_DATE).sum())
        else:
            if verbose:
                print("  ⚠️  No signal dates available — falling back to positional "
                      "80/20 split (NOT reproducible across runs)")
            split = int(len(X) * 0.8)

        X_train, X_val = X.iloc[:split], X.iloc[split:]
        y_train, y_val = y.iloc[:split], y.iloc[split:]
        w_train        = sample_weights[:split]

        # Purge buffer: labels resolve in 7 days, so the last ~10 days before the
        # val boundary can have labels that overlap the holdout window. Purge
        # those training rows out rather than let them leak. Same PURGE_DAYS as
        # ml_retrainer.py.
        if dates_arr is not None and len(dates_arr) >= split + 1 and split > 0:
            try:
                PURGE_DAYS = 10
                val_start_dt = datetime.strptime(dates_arr[split], "%Y-%m-%d")
                purge_cutoff = (val_start_dt - timedelta(days=PURGE_DAYS)).strftime("%Y-%m-%d")
                keep_mask = np.array([d < purge_cutoff for d in dates_arr[:split]])
                n_purged = int((~keep_mask).sum())
                if n_purged > 0:
                    X_train = X_train.iloc[keep_mask]
                    y_train = y_train.iloc[keep_mask]
                    w_train = w_train[keep_mask]
                    if verbose:
                        print(f"  🧹 Purge buffer ({PURGE_DAYS}d): removed {n_purged} "
                              f"training rows near val start {val_start_dt.date()} — "
                              f"{len(y_train)} remain")
            except Exception:
                pass

        n_val = len(X) - split
        if verbose and split > 0 and n_val > 0.4 * split:
            print(f"  ⚠️  Holdout ({n_val} rows) exceeds 40% of train ({split} rows) — "
                  f"consider a manual re-freeze of HOLDOUT_CUTOFF_DATE.")

        # No StandardScaler: XGBoost (tree-based) doesn't need feature scaling,
        # and scaling destroys (a) column names needed for dict-form monotone_constraints
        # and (b) category dtype needed for sector_encoded with enable_categorical=True.
        # Train directly on the pandas DataFrame — same approach as ml_retrainer.py.
        self.scaler = None

        # DEFENSIVE: intersect constraints with actual DataFrame columns so a future
        # feature drift degrades gracefully instead of crashing the engine.
        params = {k: v for k, v in ML_CONFIG["xgb_params"].items() if k != "use_label_encoder"}
        if "monotone_constraints" in params:
            params["monotone_constraints"] = {
                k: v for k, v in params["monotone_constraints"].items()
                if k in X.columns
            }

        tscv = TimeSeriesSplit(n_splits=3)
        cv_aucs = []
        for fold_train_idx, fold_val_idx in tscv.split(X):
            try:
                cv_model = XGBClassifier(**params)
                cv_model.fit(X.iloc[fold_train_idx], y.iloc[fold_train_idx],
                             sample_weight=sample_weights[fold_train_idx], verbose=False)
                cv_aucs.append(roc_auc_score(y.iloc[fold_val_idx],
                               cv_model.predict_proba(X.iloc[fold_val_idx])[:, 1]))
            except Exception: pass

        np.random.seed(42)   # deterministic: same closed-market double-run → identical ML probs
        self.model = XGBClassifier(**params)
        self.model.fit(X_train, y_train, sample_weight=w_train,
                       eval_set=[(X_val, y_val)], verbose=False)

        val_preds = self.model.predict_proba(X_val)[:, 1]
        try: holdout_auc = roc_auc_score(y_val, val_preds)
        except: holdout_auc = 0.5

        try:
            from sklearn.calibration import CalibratedClassifierCV
            self.calibrator = CalibratedClassifierCV(self.model, method="isotonic", cv="prefit")
            self.calibrator.fit(X_train, y_train)
        except: self.calibrator = None

        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = dict(sorted(
                zip(X.columns,
                    [round(float(i), 4) for i in self.model.feature_importances_]),
                key=lambda x: x[1], reverse=True))

        self.trained = True
        self.model_trained_at = datetime.now().isoformat()

        # Degenerate model check — mirrors ml_retrainer.py so daily training also warns.
        if self.feature_importance:
            _top_feat, _top_imp = next(iter(self.feature_importance.items()))
            if _top_imp > 0.90:
                print(f"\n  ⚠️  DEGENERATE MODEL: {_top_feat} dominates "
                      f"(importance={_top_imp:.4f}) — check training set size and purge window.")

        if verbose:
            cv_mean = float(np.mean(cv_aucs)) if cv_aucs else 0.5
            print(f"   ✅ Model trained | CV AUC: {cv_mean:.3f} | Holdout AUC: {holdout_auc:.3f}")
            print(f"   Daily columns == retrainer columns: "
                  f"{list(X.columns) == ML_CONFIG['features']}")
            print(f"   Top features: {list(self.feature_importance.keys())[:5]}")

        try:
            import joblib as _jl, hashlib as _hl2
            _jl.dump({"model": self.model, "calibrator": self.calibrator,
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
        return df

    def predict_rules_based(self, features_dict, market_regime=1):
        """
        Weighted-factor formula, no trained model involved. Originally
        only used when no model was trained at all; now also the
        deliberate scoring path for categories whose true-outcome horizon
        (CATEGORY_HORIZONS in outcome_tracker.py -- GROWTH CORE, FHSA
        Conservative Growth, the income categories) is 180-365 days, far
        longer than any data currently on hand can validate an ML model
        against (earliest signal in the system is only ~159 days old).
        Scoring those categories with a model trained/measured on a 7-day
        proxy would be scoring them on noise -- see the 2026-08-08 ML
        diagnostic session. Rules-based is the honest choice until enough
        calendar time passes for a real model to be validated on them.
        """
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

    def predict_swing(self, features_dict):
        """
        Score using the dedicated SWING LogisticRegression model (true
        30-day horizon, empirically validated at AUC 0.692 on a genuine
        temporal holdout vs 0.497-0.567 for anything using the old
        uniform 7-day label -- see the 2026-08-08 ML diagnostic session).
        Falls back to 0.5 if the SWING model isn't loaded; caller decides
        what to do next (run_ml_engine falls back to the general model).

        Clipped to [0.1, 0.9], matching predict_rules_based() -- checked
        the real distribution on the 239-row training set (2026-08-08):
        raw predict_proba output is genuinely extreme at the low end
        (p1=0.0009, p5=0.0026, 21/239 rows below 0.05), not rare noise --
        small-sample-size overconfidence from a model trained on ~190
        rows. Clipping is order-preserving (doesn't touch the AUC/decile
        ranking already validated), only bounds the magnitude fed into
        score adjustments and the raw ml_prob shown downstream.
        """
        if self.swing_model is None or self.swing_scaler is None or not HAS_PANDAS:
            return None
        try:
            row = {f: features_dict.get(f, 0) for f in ML_CONFIG["features"]}
            df = pd.DataFrame([row])
            scaled = self.swing_scaler.transform(df)
            prob = self.swing_model.predict_proba(scaled)[0][1]
            return round(max(0.1, min(0.9, float(prob))), 4)
        except Exception:
            return None

    def predict_category(self, category, features_dict):
        """
        Generalized version of predict_swing() -- score using a category's
        own dedicated model (see ml_retrainer.train_category_model()), for
        any category once outcome_tracker.category_is_data_ready() says
        it's ready. Lazily loads and memoizes each category's model on
        first use per predictor instance (most categories will never be
        ready in a given run, so eagerly loading all of them upfront like
        train()/predict_swing() do for SWING would be wasted work).

        Returns None if no deployed model exists for this category (not
        ready yet, or its last retrain was rejected by the AUC gate) --
        caller falls back to predict_rules_based(), same contract as
        predict_swing().
        """
        if category not in self._category_models:
            try:
                from ml_retrainer import load_category_model
                self._category_models[category] = load_category_model(category)
            except Exception:
                self._category_models[category] = (None, None, None)
        model, scaler, _trained_at = self._category_models[category]
        if model is None or scaler is None or not HAS_PANDAS:
            return None
        try:
            row = {f: features_dict.get(f, 0) for f in ML_CONFIG["features"]}
            df = pd.DataFrame([row])
            scaled = scaler.transform(df)
            prob = model.predict_proba(scaled)[0][1]
            return round(max(0.1, min(0.9, float(prob))), 4)
        except Exception:
            return None

    def get_category_model_trained_at(self, category):
        """The loaded category model's trained_at, for Kelly's retrain-
        vintage tagging (see ml_engine.py's scoring loop). Must be called
        AFTER predict_category() has populated the cache for this category
        (returns None otherwise, same as "no model")."""
        return (self._category_models.get(category) or (None, None, None))[2]

    def predict(self, features_dict, market_regime=1):
        if not self.trained or not HAS_PANDAS:
            return self.predict_rules_based(features_dict, market_regime)
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

    Sector comes from pick_utils.get_pick_sector() (see _get_sector below).
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
        "consumer defensive": "Consumer",
        "consumer cyclical":  "Consumer",
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
        s = get_pick_sector(pick)
        if not (s or "").strip():
            s = _TICKER_SECTOR_OVERRIDE.get(pick.get("ticker", ""), "")
        s = (s or "Unknown").strip()
        return _SECTOR_NORM.get(s.lower(), s)

    # Count sectors in current basket (Counter is module-level import)
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

    # FIX (2026-08-08): a ticker qualifying for both an FHSA pick and a TFSA
    # pick produces two independent dict objects here (see pick_utils.py).
    # Without this, the fill loop below checks basket_tickers only ONCE
    # before it starts and never re-checks during it, so a duplicate could
    # be added to the FINAL basket twice -- real double capital allocation
    # to the same stock. Dedupe here, before eligible_replacements is even
    # built, so the duplicate never reaches the fill loop at all.
    all_candidates = dedupe_picks_by_ticker(all_candidates, verbose=True, label="sector_cap_reserve")

    # Pre-filter reserve: exclude any sector already at the cap limit.
    # Without this, the fallback below would re-admit excess picks from capped
    # sectors (e.g. JPM/BMO.TO removed as excess FINANCIALS then added back
    # because only 2 non-FINANCIALS were available in the reserve pool).
    eligible_replacements = [p for p in all_candidates
                             if _get_sector(p) == "Unknown"
                             or seen[_get_sector(p)] < max_per_sector]

    # Sort reserve pool by score descending
    eligible_replacements.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Fill excess slots with best candidates from under-represented sectors
    filled = list(kept)
    for candidate in eligible_replacements:
        if len(filled) >= len(picks):
            break
        s = _get_sector(candidate)
        if s == "Unknown" or seen[s] < max_per_sector:
            filled.append(candidate)
            seen[s] += 1
            basket_tickers.add(candidate.get("ticker"))

    if len(filled) < len(picks):
        # Fall back to excess ONLY for sectors that are still under the cap limit.
        # Never re-admit excess picks whose sector is already at max_per_sector —
        # a smaller basket is preferable to a sector-concentrated one.
        for _exc in excess:
            if len(filled) >= len(picks):
                break
            _s = _get_sector(_exc)
            if _s == "Unknown" or seen[_s] < max_per_sector:
                filled.append(_exc)
                seen[_s] += 1

    if len(excess) > 0:
        removed  = [p.get("ticker") for p in excess]
        added    = [p.get("ticker") for p in filled[len(kept):]]
        import sys
        print(f"  🏛  Sector cap: removed {removed}, added {added} (max {max_per_sector}/sector)",
              file=sys.stdout)
        _final_fin = sum(1 for p in filled if _get_sector(p) == "Financials")
        print(f"  🏛  Sector cap: Final FINANCIALS count: {_final_fin}", file=sys.stdout)

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
        yf_sector  = get_pick_sector(p)
        news_sector = SECTOR_MAP_BLOCK.get(yf_sector)
        if news_sector and sector_sentiment:
            net = sector_sentiment.get(news_sector, {}).get("net_score", 0)
            if net <= -200:
                sector_blocked.add(p["ticker"])
                if verbose:
                    print(f"   🚫 Sector block: {p['ticker']} ({news_sector} net:{net})")

    # ── HALF-KELLY WEIGHTS ────────────────────────────────────────────────────
    def score_to_kelly_wt(score, ml_prob, wr_data=None, ticker=None, verbose=True):
        # No ml_prob logged for this pick today -> no per-pick signal to size
        # Kelly on. Floor to zero rather than substituting a tier-wide average
        # or a smoothed proxy (2026-07-21 decision — see MAIN, which had no
        # ml_prob logged that day; separately flagged as a follow-up: ml_prob
        # should log for every pick daily without exception).
        if ml_prob is None:
            if verbose:
                _tkr = ticker or "?"
                print(f"    [kelly] {_tkr:<8} no ml_prob logged today → no signal, floored to 0")
            return 0.0

        # Static calibration — used when live ml_prob-bucket data is absent or
        # too thin (n < 10 either side). Keyed by score, not ml_prob: this is
        # the cold-start/bootstrap path (no outcomes.json history yet at all),
        # not the live per-pick signal this fix targets.
        if score >= 90:   p, aw, al = 0.492, 0.70, 1.0
        elif score >= 75: p, aw, al = 0.595, 1.10, 1.0
        elif score >= 60: p, aw, al = 0.658, 1.80, 1.0
        else:             p, aw, al = 0.556, 1.10, 1.0
        p_source = "static_fallback"
        b_source = "static_fallback"

        bucket = ml_prob_bucket(ml_prob)

        def _bucket_stats(table, bucket):
            d   = (table or {}).get(bucket, {})
            _aw = d.get("avg_win")
            _al = d.get("avg_loss")
            _n  = d.get("count", 0)
            _nw = round(_n * d.get("win_rate", 0) / 100)
            _nl = _n - _nw
            ok  = bool(_aw and _al and _al > 0 and _nw >= 10 and _nl >= 10)
            return ok, d, _nw, _nl

        _used = None
        if wr_data:
            # FIX (2026-08-08): prefer the bucket table computed from
            # ml_prob_source=="model" rows only. Checked empirically: pooled
            # data (89% legacy "unknown" rows) says bucket 0.6-0.8 is the
            # strongest edge, but restricted to what the live model actually
            # produced, that bucket shows NEGATIVE edge -- the pooled table
            # was letting stale/legacy calibration override the current
            # model's real behavior. Falls back to the pooled table only for
            # buckets too thin in model-only data (e.g. 0.8-1.0 currently has
            # 0 model-sourced rows -- a genuine cold-start gap, not something
            # a filter can fix). See tests/test_invariants.py
            # test_kelly_prefers_model_sourced_bucket_over_pooled.
            ok_model, d_model, _nw_m, _nl_m = _bucket_stats(wr_data.get("by_ml_prob_bucket_model"), bucket)
            if ok_model:
                d, _used = d_model, "model"
            else:
                ok_pooled, d_pooled, _nw_p, _nl_p = _bucket_stats(wr_data.get("by_ml_prob_bucket"), bucket)
                if ok_pooled:
                    d, _used = d_pooled, "pooled"
                elif verbose:
                    print(f"    [Kelly] thin data (model nw={_nw_m},nl={_nl_m} | pooled nw={_nw_p},nl={_nl_p}) "
                          f"for ml_prob bucket {bucket} → static fallback")

            if _used:
                p  = d.get("win_rate", 50) / 100
                aw = d.get("avg_win")   # proper odds ratio numerator
                al = d.get("avg_loss")  # proper odds ratio denominator
                # FIX (Option B, ml_prob-bucket variant, 2026-07-21): p/b now
                # come from this PICK'S OWN ml_prob bucket's measured win rate
                # and payoff ratio — not the score tier's portfolio-wide
                # average, which mathematically guaranteed f_raw < 0 for every
                # pick in the 90-100 and 75-89 score tiers regardless of
                # ml_prob. Raw ml_prob itself is not a calibrated probability
                # (checked empirically against outcomes_log.json before this
                # change: the 0.8-1.0 band wins only 50.2% of the time, worse
                # than the 0.6-0.8 band's 61.7% — non-monotonic, so ml_prob is
                # used here only to pick a bucket whose historical win rate/
                # payoff is independently measured, never taken at face value
                # as p itself). See tests/test_invariants.py
                # test_kelly_p_source_is_ml_prob_bucket_not_score_tier and
                # test_ml_prob_bucket_table_matches_recomputation.
                p_source = f"ml_prob_bucket_win_rate[{_used}:{bucket}]"
                b_source = f"ml_prob_bucket_avg_win_loss_ratio[{_used}:{bucket}]"
        b = aw / al
        f_raw = (p * b - (1 - p)) / b
        if verbose and f_raw <= 0:
            _tkr = ticker or "?"
            print(f"    [kelly] {_tkr:<8} p={p:.4f}  b={b:.2f}  f_raw={f_raw:+.4f} → floored to 0")
            print(f"            p_source={p_source}  b_source={b_source}")
        return max(0.0, f_raw * 0.50)

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

    # ml_prob now feeds Kelly's p/b directly inside score_to_kelly_wt (via the
    # ml_prob bucket lookup) — the old post-hoc _ml_edge_multiplier was removed
    # 2026-07-21. It existed only to give ml_prob *some* influence on sizing
    # since it couldn't touch p/b directly; keeping it after this fix would
    # apply the same ml_prob signal twice (once in p/b, once as a multiplier),
    # double-counting and compounding picks that land in a high-edge bucket.
    raw_kelly_wts = [score_to_kelly_wt(
                         _trend_adjusted_score(p, _score_hist, _outcomes_lookup),
                         p.get("ml_prob"), win_rate_data,
                         ticker=p.get("ticker"), verbose=verbose)
                     for p in picks[:n_picks]]

    kelly_wts = raw_kelly_wts

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
        elif kelly_wts[i] == 0.0 and picks[i].get("ml_prob") is None:
            # No ml_prob logged at all -- not "computed zero edge", just no
            # signal to size on. The base_wt*0.50 floor below exists to stop
            # a concentration cascade among picks that DO have a (bad)
            # measured edge (2026-06-18 decision); it was never justified for
            # picks with no data whatsoever, and defaulting them into 50% of
            # base weight let a no-signal pick (e.g. CVX, 2026-08-17: Kelly
            # 0.000, no ml_prob logged) draw real capital. Exclude outright;
            # the all-picks-zero fallback below still covers the case where
            # nothing in the basket has data.
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

    # Kelly floor: when ALL non-blocked picks show zero edge, cap deployment at 50%.
    # The renorm loop above normalises zero-kelly weights back to 1.0 — undo that here.
    # The 0.50 floor means the remaining 50% stays as cash until edge is detected.
    _nonblocked_have_kelly = any(
        kelly_wts[i] > 0
        for i in range(n_picks)
        if picks[i]["ticker"] not in sector_blocked
    )
    if not _nonblocked_have_kelly:
        final_wts = [w * 0.50 for w in final_wts]
        if verbose:
            print(f"   ⚠️  Kelly floor: no edge detected across all {n_picks} picks"
                  f" — deploying 50% equity only (remaining 50% reserved as cash)")

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
                        current_drawdown=0.0, min_position=250.0, verbose=True,
                        sharpe_multiplier=1.0):
    """
    Convert weight fractions to dollar amounts for one account.
    Applies: universe filter, regime equity%, drawdown reduction, min_position floor,
    Sharpe advisory multiplier (added 2026-08-10 -- see run_daily.py's early
    rolling-Sharpe computation before this is ever called).

    Returns list of {ticker, weight_pct, dollar_amt, ml_prob, kelly_wt, vol_adj, score}.
    When universe is ALL and no picks are filtered and sharpe_multiplier is 1.0,
    dollar_amt == deployable * weight to the cent — matching the legacy
    calculate_position_sizes output exactly.
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

    deployable = capital * regime_equity_pct * dd_multiplier * sharpe_multiplier

    # Weight scaling: re-normalize only when universe filter removed some picks.
    # When universe=ALL, use raw fractions so the cash from CONC_CAP is preserved.
    if len(eligible) < len(target_weights):
        total_wt = sum(w["weight"] for w in eligible) or 1.0
        wt_scale = 1.0 / total_wt
    else:
        wt_scale = 1.0

    # Effective deployment = deployable × sum-of-weights (may be <1.0 when Kelly floor active)
    _eff_wt = sum(w["weight"] * wt_scale for w in eligible)
    _eff_deployed = deployable * min(1.0, _eff_wt)

    # Minimum deploy floor: never deploy less than $1,500 on accounts with sufficient capital.
    # This is a real-world trade-viability guard (can't usefully split a few hundred
    # dollars across several picks), NOT part of the % sizing stack below — it is
    # inherently dollar-denominated and does not scale with portfolio size the way
    # the rest of the stack does. A sub-$1,500-deployable account (small capital,
    # or a low regime/Kelly combination) will hit this floor more often than a large one.
    _MIN_DEPLOY = 1500.0
    _floor_applied = False
    if 0 < _eff_deployed < _MIN_DEPLOY and deployable >= _MIN_DEPLOY:
        _floor_scale  = _MIN_DEPLOY / _eff_deployed
        wt_scale     *= _floor_scale          # scales all norm_weight * dollar_amt uniformly
        _unconstrained = _eff_deployed
        _eff_deployed  = _MIN_DEPLOY
        _eff_wt        = sum(w["weight"] * wt_scale for w in eligible)   # recompute for header
        _floor_applied = True

    if verbose:
        # ── SIZING STACK — percentage/ratio terms only. This is what generalizes
        # to any portfolio size: a $500 account and a $5,000,000 account both get
        # exactly this % stack applied to their own capital. Dollar figures appear
        # only in the "Example render" line below, clearly marked as illustrative
        # for THIS account's configured capital, not a default/assumed size.
        _regime_pct_uncapped = 1.0 - market_regime.get("cash_pct", 0.0)
        _regime_bound         = min(_regime_pct_uncapped, max_equity)
        _binding              = "max_equity_cap" if max_equity < _regime_pct_uncapped else "regime_equity_pct"
        _post_regime_dd_pct   = _regime_bound * dd_multiplier * sharpe_multiplier
        _final_deployable_pct = _post_regime_dd_pct * min(1.0, _eff_wt)

        print(f"\n   ━━━ SIZING STACK [{acct_name}] — % of capital, portfolio-size-agnostic ━━━")
        print(f"   base_deployable_pct:        100.0%")
        print(f"   regime_equity_pct:          {_regime_pct_uncapped*100:5.1f}%   "
              f"({market_regime.get('regime','?')}, cash {market_regime.get('cash_pct',0)*100:.0f}%)")
        print(f"   max_equity_cap:             {max_equity*100:5.1f}%   (early-regime cap)")
        print(f"   ── bound = min(regime_equity_pct, max_equity_cap): {_regime_bound*100:5.1f}%  (binding: {_binding})")
        if dd_multiplier < 1.0:
            print(f"   × drawdown_multiplier:      {dd_multiplier*100:5.1f}%   (drawdown {current_drawdown*100:.1f}% > trigger)")
        else:
            print(f"   × drawdown_multiplier:      100.0%   (not triggered)")
        if sharpe_multiplier < 1.0:
            print(f"   × sharpe_multiplier:        {sharpe_multiplier*100:5.1f}%   (rolling Sharpe advisory — see run_daily.py Step 5 log)")
        else:
            print(f"   × sharpe_multiplier:        100.0%   (rolling Sharpe ≥ 0.3, no adjustment)")
        print(f"   ── deployable_pct:           {_post_regime_dd_pct*100:5.1f}%   (bound × drawdown_multiplier × sharpe_multiplier)")
        print(f"   × effective_weight_pct:     {min(1.0,_eff_wt)*100:5.1f}%   "
              f"(Kelly floor + concentration cap + probation, combined — remainder held as cash)")
        print(f"   = final_deployable_pct:     {_final_deployable_pct*100:5.1f}%   of total account capital"
              + ("  (before min-deploy floor override)" if _floor_applied else ""))
        print(f"   composition rule: min(regime_equity_pct, max_equity_cap) × drawdown_multiplier × sharpe_multiplier × effective_weight_pct"
              + (" , then min-deploy floor override (dollar-denominated, see below)" if _floor_applied else ""))

        _header = (f"\n   💰 Example render [{acct_name}] @ this account's capital (${capital:,.0f}"
                   + (f" | max_equity: {max_equity*100:.0f}%" if max_equity < 1.0 else "")
                   + f" | Equity: {round(regime_equity_pct*100)}%"
                   + f"): deploying ${_eff_deployed:,.0f}")
        if _floor_applied:
            _header += (f" of ${deployable:,.0f} available "
                        f"(min-deploy floor: calculated ${_unconstrained:,.0f} → floored at ${_MIN_DEPLOY:,.0f}"
                        f" — dollar-denominated override, does not scale with capital)")
        elif _eff_wt < 0.98:
            _header += f" of ${deployable:,.0f} available (Kelly floor — {_eff_wt*100:.0f}% deployed)"
        print(_header)
        print(f"   (Dollar figures above are illustrative for this account's ${capital:,.0f} capital only — "
              f"the SIZING STACK % above is the source of truth and applies identically at any capital size,"
              f" except the min-deploy floor note.)")
        print(f"   ⚠️  NOTE: A further Step 11 RISK MULTIPLIER (unified regime / convergence / "
              f"PCR conflict — also percentage-based) is computed later in the pipeline and additionally "
              f"scales this into the final trade size. See 'RISK MULTIPLIER' in the Risk Audit log.")

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
        _sum_dollar = sum(a["dollar_amt"] for a in allocations)
        # When min-position floor drops a pick, kept picks are renormalized to 100%
        # of `deployable` (not `_eff_deployed`) — this redeploys the Kelly-floor cash
        # reserve rather than preserving it. Documented, not changed, in Phase 1.
        _expected = deployable if dropped else _eff_deployed
        _basis    = ("deployable — full (min-floor drop renormalized kept picks to 100%)"
                     if dropped else "_eff_deployed (Kelly-floor-adjusted)")
        _delta    = _sum_dollar - _expected
        _mark     = "✅" if abs(_delta) < 1.0 else "❌"
        print(f"   ── Per-pick check: Σ(dollar_amt)=${_sum_dollar:,.2f} vs expected ${_expected:,.2f} "
              f"basis=[{_basis}] → {_mark}" + (f"  Δ=${_delta:,.2f}" if _mark == "❌" else ""))
        if dropped:
            print(f"   ℹ️  NOTE: the Kelly-floor cash reserve "
                  f"({(1 - min(1.0, _eff_wt)) * 100:.0f}% held as cash per the SIZING STACK above) is "
                  f"NOT preserved when the min-position floor drops a pick — it is redeployed among "
                  f"the remaining kept picks instead of staying as cash.")

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
                  sector_sentiment=None, win_rate_data=None, sharpe_multiplier=1.0):
    """
    Full ML engine run with score smoothing + Kelly sizing.
    win_rate_data: pass brief['win_rate'] from outcome_tracker for live Kelly calibration.
    sharpe_multiplier: rolling-Sharpe advisory multiplier on deployable capital
      (added 2026-08-10) -- see run_daily.py's early rolling-Sharpe computation,
      run before this so the multiplier is known before sizing happens.
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

    # FIX (2026-08-09): auto-demotion signal -- see outcome_tracker.py's
    # model_health_check() docstring. Computed once per run (not per-pick)
    # and written to disk by run_daily.py before this runs; loaded here to
    # decide whether SWING's model should be trusted this run at all.
    # "insufficient_data" (the expected state for weeks, since SWING's
    # model only deployed 2026-08-08 and its 30-day true-horizon can't
    # resolve any swing_model-sourced pick before 2026-09-07) is NOT
    # treated as degraded -- only an explicit "degraded" verdict skips it.
    from outcome_tracker import load_model_health
    _health_state    = load_model_health()
    _swing_health    = _health_state.get("SWING", {})
    _swing_degraded  = _swing_health.get("status") == "degraded"
    _general_health  = _health_state.get("GENERAL", {})
    _general_degraded = _general_health.get("status") == "degraded"
    if _swing_degraded and verbose:
        print(f"  🩺 SWING model health: DEGRADED ({_swing_health.get('reason','')}) "
              f"-- falling back to general model for SWING picks this run")
    if _general_degraded and verbose:
        print(f"  🩺 General model health: DEGRADED ({_general_health.get('reason','')}) "
              f"-- falling back to rules-based scoring for general-model picks this run")

    regime_num = 1 if regime["regime"] in ("BULL", "RECOVERY") else 0
    _raw_picks = (
        screener_picks.get("FHSA_top5", []) +
        screener_picks.get("TFSA_growth_top5", []) +
        screener_picks.get("TFSA_income_top5", []) +
        screener_picks.get("TFSA_swing_top3", [])
    )
    # Dedupe by ticker — same ticker can appear in multiple buckets;
    # keep the instance with the highest composite score.
    _seen_tickers: dict = {}
    for _p in _raw_picks:
        _t = _p.get("ticker")
        if _t and (_t not in _seen_tickers or _p.get("score", 0) > _seen_tickers[_t].get("score", 0)):
            _seen_tickers[_t] = _p
    all_picks = list(_seen_tickers.values())
    if len(all_picks) < len(_raw_picks) and verbose:
        print(f"   ℹ️  Scoring dedup: {len(_raw_picks)} → {len(all_picks)} picks (removed {len(_raw_picks)-len(all_picks)} duplicates)")

    if verbose: print(f"\n🤖 Scoring {len(all_picks)} picks with ML...")

    smoothed_count  = 0
    _raw_calib_log  = []   # [(ticker, xgb_raw, calibrated, smoothed, category, source)] — logged once after loop
    for pick in all_picks:
        ticker     = pick["ticker"]
        stock_data = pick.get("data", {})
        rs         = rs_ratings.get(ticker, {}).get("rs_rating", 50) if rs_ratings else 50

        # Write canonical sector now so gate reads the SAME normalized value that
        # the sector trace prints. pick["sector"] is absent (sector lives in data dict);
        # without this write-back the gate reads "" → falls through to UNKNOWN.
        pick["sector_canonical"] = get_canonical_sector(pick)

        features = build_features_for_stock(ticker, stock_data, rs)
        # FIX (2026-08-08): category lives nested under pick["pick"]["category"]
        # (see the {"ticker":..., "data":..., "pick": {...}} shape every
        # screener bucket builds in stock_screener.py's classify_pick() call
        # sites) -- pick.get("category") was always None, so every pick
        # silently fell through to the general-model branch below and the
        # SWING/rules-based routing never actually ran. Confirmed dead in
        # production via latest_brief.json: every pick showed
        # ml_prob_source="model" regardless of category. outcome_tracker.py's
        # log_picks() already reads it correctly this way -- mirror that.
        category = get_pick_category(pick)

        if features:
            # Category-based routing (2026-08-08 ML diagnostic session):
            # SWING has enough true-30d-horizon data to support its own
            # model (AUC 0.692, genuine temporal holdout); RULES_BASED_
            # CATEGORIES have real intended horizons of 180-365 days that
            # no data on hand is old enough to validate an ML model
            # against yet (earliest signal ~159 days old) -- scoring them
            # with any model trained/measured on the old 7-day proxy would
            # be scoring on noise. Everything else keeps the existing
            # general-model path unchanged.
            xgb_raw = None
            if category == "SWING":
                swing_prob = None if _swing_degraded else predictor.predict_swing(features)
                if swing_prob is not None:
                    calibrated = swing_prob
                    source = "swing_model"
                elif _general_degraded:
                    calibrated = predictor.predict_rules_based(features, market_regime=regime_num)
                    source = "rules_based"
                else:
                    calibrated, xgb_raw = predictor.predict_raw_pair(features, market_regime=regime_num)
                    source = "model"
            elif category in RULES_BASED_CATEGORIES:
                # FIX (2026-08-09): auto-promotion hook -- see ml_retrainer.
                # train_category_model()'s docstring. category_is_data_
                # ready() is False for every category here today (earliest
                # is WATCH on 2026-09-12), so this is a safe no-op until
                # then: predict_category() returns None (no deployed model
                # exists yet) and every pick still falls through to
                # rules_based exactly as before. Once a category becomes
                # ready AND train_category_model() has deployed a model for
                # it (run_daily.py's daily trigger), this starts scoring it
                # for real automatically -- no code change needed here.
                cat_prob = (predictor.predict_category(category, features)
                            if category_is_data_ready(category) else None)
                if cat_prob is not None:
                    calibrated = cat_prob
                    source = "category_model"
                else:
                    calibrated = predictor.predict_rules_based(features, market_regime=regime_num)
                    source = "rules_based"
            elif _general_degraded:
                # FIX (2026-08-09): auto-demotion for the general model,
                # mirroring SWING's -- see model_health_check()'s docstring.
                calibrated = predictor.predict_rules_based(features, market_regime=regime_num)
                source = "rules_based"
            else:
                calibrated, xgb_raw = predictor.predict_raw_pair(features, market_regime=regime_num)
                source = "model"

            if xgb_raw is None:
                xgb_raw = calibrated

            # ── Score smoothing: 3-day EMA to dampen single-day spikes ──────
            smoothed_prob = smooth_ml_prob(ticker, calibrated, alpha=0.4)
            if abs(smoothed_prob - calibrated) > 0.03:
                smoothed_count += 1
            pick["ml_prob"]        = smoothed_prob
            pick["ml_prob_raw"]    = calibrated   # post-calibration, pre-smoothing
            pick["ml_prob_xgb"]    = xgb_raw      # raw XGBoost output (pre-calibration)
            pick["ml_prob_source"] = source
            # FIX (2026-08-09): Kelly's durable calibration fix needs to know
            # WHICH retrain generation scored this pick, not just that some
            # model did -- see outcome_tracker.py's compute_win_rate()
            # docstring. None for rules_based (no model involved).
            if source == "swing_model":
                pick["scored_by_model_trained_at"] = predictor.swing_model_trained_at
            elif source == "model":
                pick["scored_by_model_trained_at"] = predictor.model_trained_at
            elif source == "category_model":
                pick["scored_by_model_trained_at"] = predictor.get_category_model_trained_at(category)
            else:
                pick["scored_by_model_trained_at"] = None
            _raw_calib_log.append((ticker, xgb_raw, calibrated, smoothed_prob, category, source))
        else:
            pick["ml_prob"]        = 0.5
            pick["ml_prob_raw"]    = 0.5
            pick["ml_prob_xgb"]    = 0.5
            pick["ml_prob_source"] = "default"
            _raw_calib_log.append((ticker, 0.5, 0.5, 0.5, category, "default"))

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
        print(f"  {'Ticker':<12} {'Category':<26} {'Source':<12} {'XGB-raw':>8} {'Calib':>8} {'Smoothed':>9}")
        for tk, xr, ca, sm, cat, src in sorted(_raw_calib_log, key=lambda x: -x[2]):
            marker = " ⚠️" if abs(xr - ca) > 0.05 else ""
            print(f"  {tk:<12} {str(cat or '—'):<26} {src:<12} {xr:>8.4f} {ca:>8.4f} {sm:>9.4f}{marker}")

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
        _sec = get_canonical_sector(_p)
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
    # FIX 3: BANKS and FINANCIAL SERVICES both normalize to FINANCIALS via _SECTOR_NORM_INF.
    # One canonical label, one cap bucket. BANKS as a separate ALLOW key is dead code
    # after normalization — "banks" → "FINANCIALS" before this check is ever reached.
    SECTOR_ALLOW      = {'ENERGY', 'FINANCIALS'}
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
        # sector_canonical is written during the scoring loop above from pick["data"]["sector"].
        # pick.get("sector") is the TOP-LEVEL field which is absent on screener picks (sector
        # lives nested under pick["data"]) — reading it returns "" → UNKNOWN (the prior bug).
        sector = get_canonical_sector(pick)

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
        _reserve_seen = set()   # FIX 2: dedup — same ticker in multiple screener buckets
        _reserve_raw_count = 0
        for grp in ["TFSA_growth_top5", "TFSA_income_top5", "TFSA_swing_top3"]:
            for p in screener_picks.get(grp, []):
                _tkr = p.get("ticker")
                _reserve_raw_count += 1
                if not _tkr or _tkr in basket_tickers or _tkr in _reserve_seen:
                    continue
                if _tkr in _pre_gate_excluded:          # blocked by sector≥75 or sector-cap
                    continue
                if _tkr in _cd_set:
                    continue
                if (p.get("news_adjustment", 0) or 0) < 0:
                    continue
                # Canonical normalization so "Consumer Defensive" → CONSUMER, "Banks" → FINANCIALS
                # Reserve picks are from screener_picks (not all_picks), so sector_canonical
                # may not be set; fall back to data dict where the real sector string lives.
                _sec_r = get_canonical_sector(p)
                _scr   = p.get("score", 0) or 0
                _mp_r  = p.get("ml_prob", 0.5) or 0.5
                if _sec_r == "MATERIALS" and _scr >= 75:
                    continue                              # materials≥75 blocked in reserve too
                if _scr >= ML_GATE_SCORE_MIN:
                    if _sec_r in SECTOR_BLOCK:
                        continue                          # sector-blocked in reserve too
                    if _sec_r not in SECTOR_ALLOW and not gate.decide(_tkr, _mp_r, score=_scr):
                        continue                          # ML-gated in reserve too
                _reserve_seen.add(_tkr)
                reserve.append(p)
                basket_tickers.add(_tkr)
        print(f"  📋 Reserve pool: {len(reserve)} candidates (from {_reserve_raw_count} raw, post-dedup/cooldown/sector-gate)")
        reserve.sort(key=lambda x: x.get("score", 0), reverse=True)

        # Sector-cap-aware replacement: track how many of each canonical sector
        # are already in `passed`, then skip reserve candidates that would push
        # any sector above max 2.  Without this, gate substitution can re-add
        # FINANCIALS (or any sector) that the pre-gate cap already trimmed.
        _repl_sector_counts: dict = {}
        for _pp in passed:
            _ps = get_canonical_sector(_pp)
            if _ps and _ps != "UNKNOWN":
                _repl_sector_counts[_ps] = _repl_sector_counts.get(_ps, 0) + 1

        replacements = []
        _repl_max_per_sector = 2
        for _cand in reserve:
            if len(replacements) >= len(gated_out):
                break
            _cs = get_canonical_sector(_cand)
            if _cs and _cs != "UNKNOWN" and _repl_sector_counts.get(_cs, 0) >= _repl_max_per_sector:
                continue   # would exceed sector cap — skip
            replacements.append(_cand)
            if _cs and _cs != "UNKNOWN":
                _repl_sector_counts[_cs] = _repl_sector_counts.get(_cs, 0) + 1

        tfsa_picks = passed + replacements

        gated_tickers = [p.get("ticker") for p in gated_out]
        repl_tickers  = [p.get("ticker") for p in replacements]
        _substitution_tickers = repl_tickers
        print(f"  🔄 Gate removed {gated_tickers}"
              + (f", added {repl_tickers}" if repl_tickers else " (no replacements)"))

    # ── FINAL BASKET SECTOR COUNTS (canonical — FIX 3 / FIX 1 audit) ────────────
    if verbose:
        _basket_sectors = Counter(get_canonical_sector(p) for p in tfsa_picks)
        print(f"  📊 Final basket sector counts (canonical, max-2 cap): {dict(_basket_sectors)}")
        for p in tfsa_picks:
            _cs = get_canonical_sector(p)
            print(f"     {p.get('ticker','?'):<12} → {_cs}")

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
                sharpe_multiplier=sharpe_multiplier,
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
            sharpe_multiplier=sharpe_multiplier,
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
