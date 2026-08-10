"""
InvestOS — ML Retrainer v2
===========================
Reads outcomes_log.json (1,480+ resolved picks) and retrains XGBoost
using actual 5-day forward returns as the label.

v2 changes:
  - FEATURES matches ml_engine.py ML_CONFIG exactly (22 features)
    This file is the source of truth — feature list kept in sync with ML_CONFIG
  - retrain_if_due() call in run_daily.py: weekly auto-retrain

HOW TO RUN
----------
    python ml_retrainer.py            # weekly gate (skips if <7 days since last run)
    python ml_retrainer.py --force    # force retrain now
    python ml_retrainer.py --diagnose # diagnostic report without retraining

WIRE INTO run_daily.py (add near top of ML section):
    from ml_retrainer import retrain_if_due
    retrain_if_due()   # retrains weekly, skips otherwise
"""

import json
import os
import math
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

try:
    import numpy as np
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import roc_auc_score
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import TimeSeriesSplit
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    import joblib
    HAS_JOBLIB = True
except ImportError:
    HAS_JOBLIB = False

OUTCOMES_FILE  = "outcomes_log.json"
MODEL_CACHE    = "ml_model_cache.pkl"
REPORT_FILE    = "ml_retrainer_report.json"

# Dedicated SWING model (2026-08-08) — see CATEGORY_HORIZONS in
# outcome_tracker.py. SWING is the only category with enough true-horizon
# (30d) data to support its own model right now; empirically validated at
# AUC 0.692 on a genuine temporal holdout, vs 0.497-0.567 for anything
# trained on the old uniform 7-day label. A plain LogisticRegression, not
# XGBoost, per the same session's finding that LR beats XGBoost on this
# feature set/sample size. Separate cache file from the general model —
# this one is small and cheap enough to retrain on every call rather than
# needing a weekly gate.
SWING_MODEL_CACHE  = "swing_model_cache.pkl"
SWING_MODEL_REPORT = "swing_model_report.json"
SWING_MIN_ROWS_TO_TRAIN = 80

# FIX (2026-08-09): neither train_swing_model() nor retrain() (the general
# model) previously gated deployment on holdout_auc at all -- both computed
# it, printed it, and then unconditionally overwrote the live model cache
# regardless of the value, every single call (train_swing_model runs 3x/day
# via run_daily.py, with no demotion path if a bad retrain got deployed).
# 0.53 was already used as an informal "better than random" cutoff in
# retrain()'s print statement (see the AUC log line below) -- promoted here
# from a comment to an actual enforcement gate for both models. Below this
# bar (or holdout_auc is None -- validation set too small/degenerate to
# trust at all), the retrain is REJECTED: the previously-deployed model (if
# any) stays live, and nothing is overwritten.
MIN_HOLDOUT_AUC_TO_DEPLOY = 0.53

# ── Sector normalization: yfinance raw string → canonical key ─────────────────
# Must stay in sync with _apply_sector_cap() in ml_engine.py
SECTOR_NORM = {
    "financial services":        "FINANCIALS",
    "financials":                "FINANCIALS",
    "banks":                     "FINANCIALS",
    "insurance":                 "FINANCIALS",
    "asset management":          "FINANCIALS",
    "capital markets":           "FINANCIALS",
    "diversified financials":    "FINANCIALS",
    "real estate":               "REIT",
    "reits":                     "REIT",
    "reit":                      "REIT",
    "real estate investment trusts": "REIT",
    "energy":                    "ENERGY",
    "oil & gas":                 "ENERGY",
    "oil and gas":               "ENERGY",
    "utilities":                 "UTILITIES",
    "consumer discretionary":    "CONSUMER",
    "consumer staples":          "CONSUMER",
    "consumer defensive":        "CONSUMER",   # yfinance sub-label for staples/non-cyclical
    "consumer cyclical":         "CONSUMER",   # yfinance sub-label for discretionary
    "technology":                "TECH",
    "information technology":    "TECH",
    "communication services":    "TELECOM",
    "telecommunications":        "TELECOM",
    "telecom":                   "TELECOM",
    "health care":               "HEALTHCARE",
    "healthcare":                "HEALTHCARE",
    "pharmaceuticals":           "HEALTHCARE",
    "biotechnology":             "HEALTHCARE",
    "industrials":               "INDUSTRIALS",
    "materials":                 "MATERIALS",
    "pipelines":                 "PIPELINES",
}

# FIXED integer codes — never reorder (would break feature hash and cross-retrain comparability).
# UNKNOWN uses -1; pandas Categorical handles negative codes correctly.
# Regime codes are ORDINAL (risk-ordered). Sector codes are NOMINAL
# (XGBoost categorical handles non-ordinal splits).
SECTOR_ENCODING = {
    "FINANCIALS":  0, "REIT":       1, "ENERGY":      2,
    "UTILITIES":   3, "CONSUMER":   4, "TECH":        5,
    "TELECOM":     6, "HEALTHCARE": 7, "INDUSTRIALS": 8,
    "MATERIALS":   9, "PIPELINES": 10,
    "UNKNOWN":    -1,
}

REGIME_ENCODING = {
    "CAPITAL_PRESERVATION": 0, "DEFENSIVE": 1, "NEUTRAL": 2, "RISK_ON": 3,
}

MACRO_ENCODING = {
    "RISK_OFF": 0, "BEAR": 0, "CAUTIOUS": 1, "NORMAL": 2, "RISK_ON": 3, "BULL": 3,
}
RETRAIN_LOCK   = "ml_last_retrain.txt"

# Frozen walk-forward split boundary (Phase 1 fix, 2026-07-23): rows with
# signal_date <= this are ALWAYS train, later dates are ALWAYS holdout. Previously
# the split was positional (X.iloc[:int(n*0.8)]) on a growing date-sorted array, so
# "holdout" was a different population every retrain and AUC numbers weren't
# comparable across runs. This date must stay fixed until a deliberate re-freeze
# (see the holdout/train ratio warning in train_and_save).
HOLDOUT_CUTOFF_DATE = "2026-06-19"

# CRITICAL: must match ML_CONFIG["features"] in ml_engine.py exactly
# If you add features here, also add them there — and vice versa.
FEATURES = [
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
    # Regime context features — wired into log_picks() 2026-07-04
    # unified_regime/macro_regime: ordinal-encoded (0=worst, 3=best)
    # market_breadth_50ma: normalized [0,1]. Coverage grows as new picks resolve.
    # July 6 retrain: market_breadth_50ma=12% usable; others default (constant) → OK.
    # August retrain: all three will have real variance as fresh picks resolve.
    "unified_regime_enc",    # CAPITAL_PRESERVATION=0, DEFENSIVE=1, NEUTRAL=2, RISK_ON=3
    "macro_regime_enc",      # RISK_OFF/BEAR=0, CAUTIOUS=1, NORMAL=2, RISK_ON/BULL=3
    "market_breadth_50ma",   # pct of universe above 50MA, normalized to [0,1]
    # sector_encoded removed: model was degenerate (importance 1.0000). Sector logic
    # lives in the gate (SECTOR_ALLOW / SECTOR_BLOCK). Do NOT re-add.
    # sector_momentum, market_regime, close_to_ema20_ratio removed (2026-08-08):
    # confirmed constant (zero variance) across all 2465 training rows -- a
    # data-pipeline bug (never actually computed per-row), pure dead weight. Must
    # stay in sync with ml_engine.py's ML_CONFIG['features'] -- feature_hash
    # compatibility between the two files depends on it.
]

XGB_PARAMS = {
    "n_estimators":     150,
    "max_depth":        3,
    "learning_rate":    0.04,
    "subsample":        0.8,
    "colsample_bytree": 0.7,
    "min_child_weight": 4,
    "reg_alpha":        0.15,
    "reg_lambda":       1.0,
    "random_state":     42,
    "eval_metric":      "auc",
    "verbosity":        0,
    # enable_categorical removed with sector_encoded — no categorical features remain
    # Monotonic constraints — DICT-FORM (works because training uses pandas DataFrame,
    # so XGBoost sees column names, not numpy auto-names f0,f1,...).
    # Unmentioned features default to 0 (unconstrained). Regime features are ordinal
    # (higher = more bullish) but left unconstrained to allow non-monotonic interactions.
    "monotone_constraints": {
        # roe flipped +1 -> -1 (2026-08-08): empirically confirmed negative
        # correlation with the label (-0.096) in the live dataset -- the old
        # +1 constraint was forcing the model to fit the opposite of what the
        # data actually shows.
        "roe":                  -1,
        "profit_margin":         1,   # higher margin → better
        "earnings_yield":        1,   # higher E/P → cheaper → better
        "volatility_90d":       -1,   # higher vol → worse risk-adj returns
    },
}

MIN_ROWS_TO_TRAIN  = 80
RETRAIN_EVERY_DAYS = 7


def load_resolved_outcomes():
    if not os.path.exists(OUTCOMES_FILE):
        print(f"  ⚠️  {OUTCOMES_FILE} not found.")
        return []
    try:
        with open(OUTCOMES_FILE) as f:
            raw = json.load(f)
    except Exception as e:
        print(f"  ⚠️  Failed to read {OUTCOMES_FILE}: {e}")
        return []
    resolved = [o for o in raw
                if o.get("resolved") is True
                and o.get("actual_return") is not None
                and o.get("outcome") in ("WIN","LOSS","FLAT")]
    n_default = sum(1 for o in resolved if o.get("ml_prob_source") == "default")
    print(f"  📂 {OUTCOMES_FILE}: {len(raw)} total rows, {len(resolved)} resolved "
          f"({n_default} crash-path ml_prob=0.5 — exclude these for ml_prob attribution)")
    return resolved


def build_feature_matrix(resolved):
    if not HAS_PANDAS:
        print("  ⚠️  pandas not available.")
        return None, None, None, None

    returns = [o.get("actual_return", 0) or 0 for o in resolved]
    median_return = float(np.median(returns))
    print(f"  📊 Label threshold: actual_return > {median_return:.2f}% (median)")

    rows_X, rows_y, weights, dates_list = [], [], [], []
    now_ts = datetime.now().timestamp()

    for o in sorted(resolved, key=lambda x: x.get("signal_date","2020-01-01")):
        mom_6m      = (o.get("perf_90d", 0) or 0) / 100
        vol_raw     = max(o.get("volatility", 2) or 2, 0.5) / 100
        roe         = min(o.get("roe", 0) or 0, 100) / 100
        pm          = min(o.get("profit_margin", 0) or 0, 100) / 100
        pe          = o.get("pe_ratio", 20) or 20
        div_yield   = (o.get("div_yield", 0) or 0) / 100
        rev_growth  = (o.get("rev_growth", 0) or 0) / 100
        earn_growth = (o.get("earn_growth", 0) or 0) / 100
        debt_equity = min(o.get("debt_equity", 1) or 1, 10) / 10
        # rs_rating: use 0 as sentinel for missing (not 50 which inflates coverage gate)
        rs_rating   = (o.get("rs_rating") or 0) / 100

        vol_adj_mom  = float(np.clip(mom_6m / max(vol_raw, 0.01), -5.0, 5.0))
        earnings_yield = 1.0 / max(pe, 1)
        fcf_yield    = max(pm * 0.8, 0)
        beta         = min(vol_raw / 0.15, 3.0)

        # Regime features
        regime_str   = o.get("regime", "BULL") or "BULL"
        market_regime = 1.0 if regime_str.upper() in ("BULL","RECOVERY") else 0.0
        spx_pct      = o.get("spx_vs_ma200", 0) or 0
        spx_vs_ma200 = float(np.clip(spx_pct / 20.0, -1.0, 1.0))
        raw_boost    = o.get("news_boost", 0) or o.get("news_adjustment", 0) or 0
        news_boost   = float(np.clip(raw_boost / 20.0, -1.0, 1.0))

        # Unified + macro regime: ordinal-encoded via module-level dicts
        ur_raw             = (o.get("unified_regime") or "NEUTRAL").upper()
        mr_raw             = (o.get("macro_regime") or "NORMAL").upper()
        unified_regime_enc = float(REGIME_ENCODING.get(ur_raw, 2))   # default NEUTRAL=2
        macro_regime_enc   = float(MACRO_ENCODING.get(mr_raw, 2))    # default NORMAL=2

        # Sector: normalize raw yfinance string → canonical key → integer code
        sector_raw = (o.get("sector") or "").strip().lower()
        sector_key = SECTOR_NORM.get(sector_raw, "UNKNOWN")
        sector_enc = SECTOR_ENCODING.get(sector_key, -1)  # int; -1 = UNKNOWN

        # Market breadth above 50MA — normalized [0,1]; default 0.5 (unknown)
        _b50 = o.get("market_breadth_50ma")
        market_breadth_50ma = float(_b50) / 100.0 if _b50 is not None else 0.5

        # close_to_ema20_ratio: logged for new picks; default 1.0 for legacy picks
        close_to_ema20_ratio = float(np.clip(
            o.get("close_to_ema20_ratio", 1.0) or 1.0, 0.5, 2.0
        ))

        feat = {
            "momentum_6m":          round(mom_6m, 4),
            "momentum_12m":         round(mom_6m * 1.4, 4),
            "vol_adj_momentum":     round(vol_adj_mom, 4),
            "roe":                  round(roe, 4),
            "profit_margin":        round(pm, 4),
            "earnings_yield":       round(earnings_yield, 4),
            "fcf_yield":            round(fcf_yield, 4),
            "volatility_90d":       round(vol_raw, 4),
            "beta":                 round(beta, 4),
            "rev_growth":           round(rev_growth, 4),
            "earn_growth":          round(earn_growth, 4),
            "div_yield":            round(div_yield, 4),
            "debt_equity":          round(debt_equity, 4),
            "rs_rating":            round(rs_rating, 4),
            "sector_momentum":      0.0,
            "market_regime":        market_regime,
            "spx_vs_ma200":         round(spx_vs_ma200, 4),
            "news_boost":           round(news_boost, 4),
            "close_to_ema20_ratio": round(close_to_ema20_ratio, 4),
            "unified_regime_enc":   round(unified_regime_enc, 4),
            "macro_regime_enc":     round(macro_regime_enc, 4),
            "market_breadth_50ma":  round(market_breadth_50ma, 4),
            "sector_encoded":       sector_enc,  # int, set to 'category' after DataFrame build
        }

        actual_return = o.get("actual_return", 0) or 0
        label = 1 if actual_return > median_return else 0
        sig_date = o.get("signal_date", "2020-01-01")

        try:
            sig_ts   = datetime.strptime(sig_date, "%Y-%m-%d").timestamp()
            days_old = max((now_ts - sig_ts) / 86400, 0)
            w        = float(np.exp(-math.log(2) / 90.0 * days_old))
        except Exception:
            w = 0.5

        rows_X.append(feat)
        rows_y.append(label)
        weights.append(max(w, 0.05))
        dates_list.append(sig_date)

    # Build full DataFrame including sector_encoded (used ONLY for imputation grouping).
    # sector_encoded is NOT in FEATURES and is dropped before returning.
    X_full = pd.DataFrame(rows_X)
    X = X_full[FEATURES]   # 22 features — used for coverage gate below
    y = pd.Series(rows_y, dtype=int)
    w = np.array(weights, dtype=float)

    pos_rate = float(y.mean())
    print(f"  ✅ Feature matrix: {len(y)} rows × {len(FEATURES)} features")
    print(f"     Label balance: {pos_rate:.1%} positive | {1-pos_rate:.1%} negative")

    # ── Part D: per-feature coverage report ──────────────────────────────────
    print(f"  📊 Feature coverage (non-null / non-default):")
    print(f"     REGIME_ENCODING : {REGIME_ENCODING}")
    print(f"     MACRO_ENCODING  : {MACRO_ENCODING}")
    print(f"     SECTOR_ENCODING : {SECTOR_ENCODING}")
    for feat in FEATURES:
        col = X[feat]
        if feat == "market_breadth_50ma":
            n_non = int((col != 0.5).sum())
            print(f"     {feat:<25} {n_non:>5}/{len(X)} ({100*n_non/len(X):>4.0f}%) non-default(0.5)")
        else:
            n_non = int((col.abs() > 0.001).sum())
            print(f"     {feat:<25} {n_non:>5}/{len(X)} ({100*n_non/len(X):>4.0f}%) non-zero")

    # ── COVERAGE GATE ─────────────────────────────────────────────────────
    # Historical picks (pre feature-capture era) have perf_90d=0, roe=0 etc.
    # Guard: require ≥10% of rows have real feature data before retraining so
    # there is enough seed data for sector-median imputation to be meaningful.
    key_features = ["momentum_6m", "roe", "profit_margin"]  # rs_rating excluded — default 50 inflates coverage
    has_real_data = X[key_features].abs().sum(axis=1) > 0.001
    coverage_pct  = float(has_real_data.mean()) * 100
    real_rows     = int(has_real_data.sum())
    print(f"  📊 Raw feature coverage (pre-imputation): {coverage_pct:.1f}% ({real_rows}/{len(y)} rows have real data)")

    # Per-feature zero check — shows which features are missing
    for feat in key_features:
        feat_coverage = float((X[feat].abs() > 0.001).mean()) * 100
        status = "✅" if feat_coverage > 5 else "⚠️  ZERO"
        print(f"     {feat:<20} {feat_coverage:>5.1f}% non-zero  {status}")

    MIN_COVERAGE_PCT = 10.0   # ~160 picks needed (10% of 1,600 historical)
    if coverage_pct < MIN_COVERAGE_PCT:
        print(f"  ⛔ Coverage {coverage_pct:.1f}% < {MIN_COVERAGE_PCT}% — preserving cached model.")
        print(f"     ~{max(0, int(MIN_COVERAGE_PCT/100 * len(y)) - real_rows)} more feature-complete picks needed.")
        print(f"     New picks capturing features daily. Auto-retrain fires when threshold crossed.")
        return None, None, None, None   # signals train_and_save to abort
    print(f"  ✅ Raw coverage {coverage_pct:.1f}% ≥ {MIN_COVERAGE_PCT}% — proceeding with imputation + retrain")
    # ──────────────────────────────────────────────────────────────────────

    # ── Sector-median imputation ──────────────────────────────────────────
    # Problem: 80%+ of historical rows have zero momentum/fundamentals (pre-capture era).
    # Training on all-zero rows → zero-variance features → AUC ≈ 0.500 (random).
    # Fix: replace zeros with the sector-level median computed from the real-data rows.
    # Groups use sector_encoded (int code, -1 = UNKNOWN). If sector median is also 0,
    # fall back to the global median across all non-zero rows.
    # sector_encoded is dropped from X after this step — it is NOT a model feature.
    IMPUTE_FEATURES = [
        "momentum_6m", "momentum_12m", "vol_adj_momentum",
        "roe", "profit_margin", "fcf_yield",
        "rev_growth", "earn_growth", "div_yield", "rs_rating",
    ]
    _before_m6m = float((X_full["momentum_6m"].abs() > 0.001).mean()) * 100
    _before_roe  = float((X_full["roe"].abs() > 0.001).mean()) * 100
    for feat in IMPUTE_FEATURES:
        zero_mask = X_full[feat].abs() < 0.001
        if zero_mask.sum() == 0:
            continue
        non_zero = X_full.loc[~zero_mask]
        global_med = float(non_zero[feat].median()) if len(non_zero) > 0 else 0.0
        sector_meds = non_zero.groupby("sector_encoded")[feat].median()
        fill_vals = X_full.loc[zero_mask, "sector_encoded"].map(sector_meds).fillna(global_med)
        X_full.loc[zero_mask, feat] = fill_vals.values
    _after_m6m = float((X_full["momentum_6m"].abs() > 0.001).mean()) * 100
    _after_roe  = float((X_full["roe"].abs() > 0.001).mean()) * 100
    print(f"  🔧 Sector-median imputation:")
    print(f"     momentum_6m  {_before_m6m:.0f}% → {_after_m6m:.0f}% non-zero")
    print(f"     roe          {_before_roe:.0f}% → {_after_roe:.0f}% non-zero")
    # ──────────────────────────────────────────────────────────────────────

    # Drop sector_encoded — it is NOT in FEATURES and must not enter the model
    X = X_full[FEATURES].copy()

    dates_arr = np.array(dates_list)
    return X, y, w, dates_arr


def train_and_save(X, y, w, dates=None):
    if not (HAS_XGB and HAS_SKLEARN and HAS_JOBLIB and HAS_PANDAS):
        print("  ⚠️  Missing libraries — cannot train.")
        return None

    n = len(y)

    # Frozen date-based split (not positional — see HOLDOUT_CUTOFF_DATE comment above).
    # Rows are already sorted ascending by signal_date (build_feature_matrix), so
    # counting dates <= cutoff gives the same prefix/suffix slicing the rest of this
    # function expects, but the boundary no longer moves as n grows.
    if dates is not None and len(dates) == n:
        dates_arr = np.asarray(dates)
        split = int((dates_arr <= HOLDOUT_CUTOFF_DATE).sum())
    else:
        print("  ⚠️  No signal dates available — falling back to positional 80/20 split "
              "(NOT reproducible across runs)")
        split = int(n * 0.8)

    X_train, X_val = X.iloc[:split], X.iloc[split:]
    y_train, y_val = y.iloc[:split], y.iloc[split:]
    w_train        = w[:split]

    n_val = n - split
    if split > 0 and n_val > 0.4 * split:
        print(f"  ⚠️  Holdout ({n_val} rows) exceeds 40% of train ({split} rows) — "
              f"consider a manual re-freeze of HOLDOUT_CUTOFF_DATE (currently "
              f"{HOLDOUT_CUTOFF_DATE}).")

    # ── Purge buffer ──────────────────────────────────────────────────────
    # Labels resolve in 7 days. Purge = label horizon + 3d buffer = 10d.
    # Point-in-time features (90d vol, 200d MA) do NOT leak across the split —
    # they are computed from prices available at signal_date. Only the label
    # (outcome 7 days later) can leak, so the purge window is label-sized, not
    # feature-lookback-sized. The old 90d window removed 1355/1682 training rows,
    # leaving only 327 and producing a degenerate single-feature model (AUC 0.454).
    PURGE_DAYS = 10
    if dates is not None and len(dates) >= split + 1:
        try:
            from datetime import datetime as _dtp, timedelta as _tdp
            val_start_dt   = _dtp.strptime(dates[split], "%Y-%m-%d")
            purge_cutoff   = (val_start_dt - _tdp(days=PURGE_DAYS)).strftime("%Y-%m-%d")
            keep_mask      = np.array([d < purge_cutoff for d in dates[:split]])
            n_purged       = int((~keep_mask).sum())
            if n_purged > 0:
                X_train = X_train.iloc[keep_mask]
                y_train = y_train.iloc[keep_mask]
                w_train = w_train[keep_mask]
                print(f"  🧹 Purge buffer ({PURGE_DAYS}d): removed {n_purged} training rows "
                      f"near val start {val_start_dt.date()} — {len(y_train)} remain")
            else:
                print(f"  🧹 Purge buffer ({PURGE_DAYS}d): no rows in contamination window")
        except Exception as _pe:
            print(f"  ⚠️  Purge buffer skipped ({_pe})")
    # ──────────────────────────────────────────────────────────────────────

    # No StandardScaler — XGBoost (tree-based) needs no feature scaling.
    # Training directly on the pandas DataFrame lets XGBoost see column names,
    # which is required for dict-form monotone_constraints.

    n_splits  = min(5, max(3, n // 50))
    tscv      = TimeSeriesSplit(n_splits=n_splits)
    cv_aucs   = []

    for fold_train_idx, fold_val_idx in tscv.split(X):
        try:
            fm = XGBClassifier(**XGB_PARAMS)
            fm.fit(X.iloc[fold_train_idx], y.iloc[fold_train_idx],
                   sample_weight=w[fold_train_idx], verbose=False)
            fa = roc_auc_score(y.iloc[fold_val_idx],
                               fm.predict_proba(X.iloc[fold_val_idx])[:,1])
            cv_aucs.append(fa)
        except Exception:
            pass

    cv_mean = float(np.mean(cv_aucs)) if cv_aucs else 0.5
    cv_std  = float(np.std(cv_aucs))  if cv_aucs else 0.0
    print(f"  📊 CV AUC: {cv_mean:.3f} ± {cv_std:.3f} ({n_splits} folds)")

    model = XGBClassifier(**XGB_PARAMS)
    model.fit(X_train, y_train, sample_weight=w_train,
              eval_set=[(X_val, y_val)], verbose=False)

    val_probs   = model.predict_proba(X_val)[:,1]
    holdout_auc = roc_auc_score(y_val, val_probs) if len(y_val.unique())>1 else 0.5
    print(f"  📊 Holdout AUC: {holdout_auc:.3f}  "
          f"({'✅ better than random' if holdout_auc > 0.53 else '⚠️ near random — more data needed'})")

    try:
        calibrator = CalibratedClassifierCV(model, method="isotonic", cv="prefit")
        calibrator.fit(X_train, y_train)
        print("  ✅ Probability calibration: isotonic")
    except Exception:
        calibrator = None

    # ── Probability distribution diagnostic ──────────────────────────────────
    # Saturation check: p10=0.25, median=0.50, p90=0.50 means the model barely
    # discriminates (all probs clustered at 0.50). Sector as a feature should
    # widen the spread. Watch this after each retrain.
    try:
        _all_probs = (calibrator if calibrator else model).predict_proba(X)[:,1]
        _p10  = float(np.percentile(_all_probs, 10))
        _p50  = float(np.percentile(_all_probs, 50))
        _p90  = float(np.percentile(_all_probs, 90))
        _sprd = round(_p90 - _p10, 3)
        _sat  = "⚠️  SATURATED — discriminates poorly" if _sprd < 0.10 else "✅ adequate spread"
        print(f"  📊 ML prob distribution: p10={_p10:.3f}  p50={_p50:.3f}  p90={_p90:.3f}"
              f"  spread={_sprd:.3f}  {_sat}")
    except Exception as _de:
        print(f"  ⚠️  Prob distribution diagnostic failed: {_de}")

    feat_imp = {}
    if hasattr(model, "feature_importances_"):
        feat_imp = dict(sorted(
            zip(FEATURES, [round(float(v),4) for v in model.feature_importances_]),
            key=lambda x: x[1], reverse=True
        ))
    print(f"  📊 Feature importances (top {min(10, len(feat_imp))}):")
    for fname, fimp in list(feat_imp.items())[:10]:
        bar = "█" * int(fimp * 200)
        print(f"     {fname:<25} {fimp:.4f}  {bar}")
    # ── Degenerate model check ────────────────────────────────────────────────
    # A single feature dominating at >0.90 means the training set is too small
    # or the constraints are misaligned — the model becomes a lookup table for
    # that feature. Root cause: purge window too wide → shrinks training set.
    if feat_imp:
        top_feat, top_imp = next(iter(feat_imp.items()))
        if top_imp > 0.90:
            print(f"\n  ⚠️  DEGENERATE MODEL: {top_feat} dominates (importance={top_imp:.4f})")
            print(f"     Check training set size and purge window.")
            print(f"     Training rows: {len(y_train)} — minimum recommended: 300+")

    # Calibration check
    calib_check = {}
    try:
        all_probs = calibrator.predict_proba(X)[:,1] if calibrator else model.predict_proba(X)[:,1]
        df_check  = pd.DataFrame({"prob": all_probs, "label": y.values})
        # rank(method="first") breaks ties before qcut. Sector-median imputation
        # gives many rows identical predicted probabilities, which collide at
        # quantile bin edges — qcut(labels=[...], duplicates="drop") then raises
        # "Bin labels must be one fewer than the number of bin edges" on most
        # runs. That was previously swallowed by a bare `except: pass` below,
        # leaving calib_check silently empty in production. Ranking first makes
        # every value unique, so qcut always produces exactly 5 populated bins.
        df_check["decile"] = pd.qcut(df_check["prob"].rank(method="first"), q=5,
                                      labels=["Q1","Q2","Q3","Q4","Q5"])
        for q, grp in df_check.groupby("decile", observed=True):
            calib_check[str(q)] = {
                "mean_pred_prob":  round(float(grp["prob"].mean()), 3),
                "actual_win_rate": round(float(grp["label"].mean()), 3),
                "count": len(grp),
            }
    except Exception as _cce:
        print(f"  ⚠️  Calibration check failed: {_cce}")

    if calib_check:
        print("  📊 Calibration check (Q1=lowest predicted, Q5=highest):")
        for q, d in calib_check.items():
            bar = "█" * int(d["actual_win_rate"] * 20)
            print(f"      {q}: pred={d['mean_pred_prob']:.2f}  actual_WR={d['actual_win_rate']:.2f}  {bar}")
        q_vals = list(calib_check.values())
        if len(q_vals) >= 2 and q_vals[-1]["actual_win_rate"] > q_vals[0]["actual_win_rate"]:
            print("  ✅ Calibration direction: CORRECT (high pred → high WR)")
        else:
            print("  ⚠️  Calibration direction: INVERTED — add more resolved picks")

    import hashlib
    feat_hash = hashlib.md5(str(FEATURES).encode()).hexdigest()[:8]

    payload = {
        "model":              model,
        "scaler":             None,  # removed — XGBoost tree needs no scaling
        "calibrator":         calibrator,
        "feature_importance": feat_imp,
        "feature_hash":       feat_hash,
        "_trained_on":        "real_outcomes",
        "_retrained_at":      datetime.now().isoformat(),
        "_n_training_rows":   len(y_train),
        "_holdout_auc":       round(holdout_auc, 4),
        "_cv_auc":            round(cv_mean, 4),
        "_label_definition":  "actual_return > median(actual_return)",
        "_features":          FEATURES,
        "_sector_encoding":   SECTOR_ENCODING,
        "_regime_encoding":   REGIME_ENCODING,
        "_macro_encoding":    MACRO_ENCODING,
    }
    report = {
        "retrained_at":       datetime.now().isoformat(),
        "n_rows":             n,
        "n_train":            split,
        "n_val":              n - split,
        "holdout_cutoff_date": HOLDOUT_CUTOFF_DATE,
        "holdout_auc":        round(holdout_auc, 4),
        "cv_auc_mean":        round(cv_mean, 4),
        "cv_auc_std":         round(cv_std, 4),
        "feature_importance": feat_imp,
        "calibration_check":  calib_check,
        "features_used":      FEATURES,
        "label_definition":   "actual_return > median(actual_return)",
    }
    # FIX (2026-08-09): deploy gate -- see MIN_HOLDOUT_AUC_TO_DEPLOY comment.
    # Previously this always deployed regardless of holdout_auc (the
    # "better than random" print above was informational only, not
    # enforced).
    if holdout_auc < MIN_HOLDOUT_AUC_TO_DEPLOY:
        report["deployed"] = False
        report["rejected_reason"] = f"holdout_auc {round(holdout_auc,4)} < minimum {MIN_HOLDOUT_AUC_TO_DEPLOY}"
        with open(REPORT_FILE, "w") as f:
            json.dump(report, f, indent=2)
        print(f"  ⛔ Retrain REJECTED (n={n}): {report['rejected_reason']} "
              f"-- keeping previously-deployed model.")
        return report

    joblib.dump(payload, MODEL_CACHE)
    print(f"  💾 Saved to {MODEL_CACHE} (feature_hash: {feat_hash})")

    report["deployed"] = True
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  📄 Report saved to {REPORT_FILE}")
    return report


def retrain(verbose=True):
    print("\n" + "="*55)
    print("  ML RETRAINER v2")
    print("="*55)

    if not (HAS_PANDAS and HAS_XGB and HAS_SKLEARN and HAS_JOBLIB):
        print("  ⚠️  Required libraries not available.")
        return None

    resolved = load_resolved_outcomes()
    if len(resolved) < MIN_ROWS_TO_TRAIN:
        print(f"  ⚠️  Only {len(resolved)} resolved picks (need {MIN_ROWS_TO_TRAIN}). Skipping.")
        return None

    X, y, w, dates = build_feature_matrix(resolved)
    if X is None or len(y) < MIN_ROWS_TO_TRAIN:
        return None

    report = train_and_save(X, y, w, dates)

    with open(RETRAIN_LOCK, "w") as f:
        f.write(datetime.now().isoformat())

    if verbose and report:
        print(f"\n  ✅ Retrain complete")
        print(f"     Rows: {report['n_rows']}  |  Holdout AUC: {report['holdout_auc']}  |  CV AUC: {report['cv_auc_mean']}")
        top3 = list(report["feature_importance"].items())[:3]
        print(f"     Top features: {[f'{k}={v}' for k,v in top3]}")

    return report


def retrain_if_due(force=False):
    """Weekly retrain gate. Call from run_daily.py: retrain_if_due()"""
    if not force and os.path.exists(RETRAIN_LOCK):
        try:
            last = datetime.fromisoformat(open(RETRAIN_LOCK).read().strip())
            days_since = (datetime.now() - last).days
            if days_since < RETRAIN_EVERY_DAYS:
                print(f"  ⏭  ML retrain: last {days_since}d ago (next in {RETRAIN_EVERY_DAYS-days_since}d)")
                return None
        except Exception:
            pass
    return retrain()


def _load_category_true_horizon_outcomes(category):
    """
    Load outcomes_log.json entries for one category that have reached
    their true CATEGORY_HORIZONS horizon (see outcome_tracker.py), aliased
    into the shape build_feature_matrix() expects: true_horizon_return ->
    actual_return, true_horizon_outcome -> outcome. Does not touch the
    original 7-day resolved/actual_return fields on disk -- this only
    reshapes an in-memory copy for feature building.
    """
    if not os.path.exists(OUTCOMES_FILE):
        return []
    try:
        with open(OUTCOMES_FILE) as f:
            raw = json.load(f)
    except Exception:
        return []
    out = []
    for o in raw:
        if o.get("category") != category or not o.get("true_horizon_resolved"):
            continue
        o2 = dict(o)
        o2["actual_return"] = o["true_horizon_return"]
        o2["outcome"]       = o["true_horizon_outcome"]
        out.append(o2)
    return out


def _category_model_paths(category):
    """Cache/report filenames for a category's dedicated model. SWING keeps
    its original, pre-existing filenames (swing_model_cache.pkl /
    swing_model_report.json) for backward compatibility with everything
    that already reads them (predict_swing, load_swing_model, dashboards,
    existing tests) -- every other category gets a generated name."""
    if category == "SWING":
        return SWING_MODEL_CACHE, SWING_MODEL_REPORT
    slug = category.replace(" ", "_").replace("+", "plus").lower()
    return f"category_model_{slug}.pkl", f"category_model_{slug}_report.json"


def train_category_model(category, min_rows=None, verbose=True):
    """
    Generalized version of train_swing_model() -- trains a dedicated
    LogisticRegression model on ONE category's true-horizon outcomes
    (CATEGORY_HORIZONS in outcome_tracker.py), for any category, not just
    SWING. This is the training path outcome_tracker.category_is_data_
    ready() implies is needed once it turns true for a category: being
    data-ready doesn't mean the pooled general model (trained on the
    legacy 7-day-proxy label) becomes valid for that category -- it needs
    its OWN model trained on its OWN true-horizon labels, same reasoning
    that justified SWING getting a dedicated model in the first place.

    Same deploy gate as train_swing_model() (MIN_HOLDOUT_AUC_TO_DEPLOY) --
    a retrain that doesn't clear the bar is rejected, not deployed, and
    the previously-deployed model (if any) is left untouched.

    min_rows defaults to MIN_TRUE_HORIZON_ROWS_FOR_ML (outcome_tracker.py's
    80-row bar) unless overridden -- kept as a parameter, not a hardcoded
    import-time constant, so a category with different data economics can
    use a different bar without a code change.
    """
    if min_rows is None:
        from outcome_tracker import MIN_TRUE_HORIZON_ROWS_FOR_ML
        min_rows = MIN_TRUE_HORIZON_ROWS_FOR_ML
    cache_path, report_path = _category_model_paths(category)
    from outcome_tracker import CATEGORY_HORIZONS, DEFAULT_HORIZON_DAYS
    horizon_days = CATEGORY_HORIZONS.get(category, DEFAULT_HORIZON_DAYS)

    if not (HAS_PANDAS and HAS_SKLEARN and HAS_JOBLIB):
        if verbose: print(f"  ⚠️  Required libraries not available.")
        return None

    resolved = _load_category_true_horizon_outcomes(category)
    if len(resolved) < min_rows:
        if verbose:
            print(f"  ⚠️  Only {len(resolved)} true-horizon {category} picks "
                  f"(need {min_rows}). Skipping {category} model.")
        return None

    X, y, w, dates = build_feature_matrix(resolved)
    if X is None or len(y) < min_rows:
        return None

    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import roc_auc_score

    n = len(y)
    split = int(n * 0.8)
    X_tr, X_val = X.iloc[:split], X.iloc[split:]
    y_tr, y_val = y.iloc[:split], y.iloc[split:]
    w_tr = w[:split]

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)

    model = LogisticRegression(penalty="l2", C=1.0, max_iter=1000, class_weight="balanced")
    model.fit(X_tr_s, y_tr, sample_weight=w_tr)

    holdout_auc = None
    if len(set(y_val)) >= 2 and len(y_val) >= 10:
        X_val_s = scaler.transform(X_val)
        holdout_auc = round(float(roc_auc_score(y_val, model.predict_proba(X_val_s)[:, 1])), 4)

    report = {
        "trained_at":   datetime.now().isoformat(),
        "model_type":   "LogisticRegression (penalty=l2, C=1.0, class_weight=balanced)",
        # FIX (2026-08-09): report had no way to judge whether holdout_auc was
        # good without knowing what it's an AUC of -- what the model predicts,
        # over what horizon, trained on what features/split. Added so the
        # report is self-describing.
        "description":  (
            f"Binary WIN/LOSS classifier for the {category} category, trained "
            f"on true {horizon_days}-day-horizon outcomes (not the legacy "
            f"7-day proxy -- see CATEGORY_HORIZONS in outcome_tracker.py). "
            f"80/20 chronological train/holdout split, no shuffling. "
            f"holdout_auc is this model's AUC on the held-out 20%, gated at "
            f"{MIN_HOLDOUT_AUC_TO_DEPLOY} before deploy (see 'deployed' field)."
        ),
        "features_used": FEATURES,
        "n_rows":       n,
        "n_train":      split,
        "n_val":        n - split,
        "holdout_auc":  holdout_auc,
        "horizon_days": horizon_days,
        "category":     category,
    }
    if holdout_auc is None or holdout_auc < MIN_HOLDOUT_AUC_TO_DEPLOY:
        report["deployed"] = False
        report["rejected_reason"] = (
            "holdout_auc is None (validation set too small/degenerate)" if holdout_auc is None
            else f"holdout_auc {holdout_auc} < minimum {MIN_HOLDOUT_AUC_TO_DEPLOY}"
        )
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        if verbose:
            print(f"  ⛔ {category} retrain REJECTED (n={n}): {report['rejected_reason']} "
                  f"-- keeping previously-deployed model, if any.")
        return report

    # Refit on ALL available data for the deployed model -- the holdout
    # split above exists only to report an honest AUC, not to withhold
    # data from the model that actually goes live.
    scaler_full = StandardScaler()
    X_full_s = scaler_full.fit_transform(X)
    model_full = LogisticRegression(penalty="l2", C=1.0, max_iter=1000, class_weight="balanced")
    model_full.fit(X_full_s, y, sample_weight=w)

    import hashlib
    feat_hash = hashlib.md5(str(FEATURES).encode()).hexdigest()[:8]
    payload = {
        "model":         model_full,
        "scaler":        scaler_full,
        "feature_hash":  feat_hash,
        "_features":     FEATURES,
        "_trained_at":   datetime.now().isoformat(),
        "_n_rows":       n,
        "_holdout_auc":  holdout_auc,
        "_category":     category,
    }
    import joblib
    joblib.dump(payload, cache_path)

    report["deployed"] = True
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    if verbose:
        print(f"  ✅ {category} model trained + DEPLOYED | n={n} | Holdout AUC: {holdout_auc}")
        print(f"  💾 Saved to {cache_path}")

    return report


def load_category_model(category):
    """
    Generalized version of load_swing_model() -- load the cached dedicated
    model for ANY category. Returns (model, scaler, trained_at) or
    (None, None, None) if unavailable/incompatible.
    """
    cache_path, _ = _category_model_paths(category)
    if not (HAS_JOBLIB and os.path.exists(cache_path)):
        return None, None, None
    try:
        import joblib, hashlib
        cached = joblib.load(cache_path)
        feat_hash = hashlib.md5(str(FEATURES).encode()).hexdigest()[:8]
        if cached.get("feature_hash") != feat_hash:
            return None, None, None
        return cached["model"], cached["scaler"], cached.get("_trained_at")
    except Exception:
        return None, None, None


def train_swing_model(verbose=True):
    """
    Train the dedicated SWING LogisticRegression model on true-30-day-
    horizon SWING outcomes. Unlike the general XGBoost model, retrains on
    every call (LR is cheap) rather than needing a weekly gate.

    Thin wrapper around train_category_model("SWING", ...) -- kept as a
    named function (rather than inlining the call at every SWING-specific
    site) so existing callers/tests didn't need to change when this was
    generalized to support any category, mirroring run_daily.py's
    dedupe_top_flat_picks()/pick_utils.dedupe_picks_by_ticker() precedent.
    """
    return train_category_model("SWING", min_rows=SWING_MIN_ROWS_TO_TRAIN, verbose=verbose)


def load_swing_model():
    """Load the cached SWING model+scaler for inference. Returns
    (model, scaler, trained_at) or (None, None, None) if unavailable/
    incompatible. trained_at (ISO string, from the deploy-gated
    train_swing_model()'s payload) feeds Kelly's retrain-vintage window --
    see outcome_tracker.py's compute_win_rate() docstring.

    Thin wrapper around load_category_model("SWING") -- see
    train_swing_model()'s docstring for why this stays a named function."""
    return load_category_model("SWING")


def diagnose():
    print("\n" + "="*55)
    print("  ML RETRAINER — DIAGNOSTIC")
    print("="*55)
    resolved = load_resolved_outcomes()
    if not resolved:
        return

    returns  = [o.get("actual_return", 0) or 0 for o in resolved]
    median_r = float(np.median(returns)) if HAS_PANDAS else sorted(returns)[len(returns)//2]
    wins     = [o for o in resolved if o["outcome"]=="WIN"]
    losses   = [o for o in resolved if o["outcome"]=="LOSS"]

    print(f"\n  OUTCOME DISTRIBUTION")
    print(f"  Wins: {len(wins)} ({len(wins)/len(resolved):.1%}) | "
          f"Losses: {len(losses)} ({len(losses)/len(resolved):.1%})")
    print(f"  Median return: {median_r:+.2f}%")

    print(f"\n  SCORE TIER INVERSION CHECK")
    for tier, lo, hi in [("90-100",90,100),("75-89",75,89),("60-74",60,74),("below-60",0,59)]:
        t  = [o for o in resolved if lo<=(o.get("score",0) or 0)<=hi]
        if t:
            wr  = sum(1 for o in t if o["outcome"]=="WIN") / len(t)
            avg = sum(o.get("actual_return",0) or 0 for o in t) / len(t)
            bar = "█" * int(wr*20)
            print(f"  Score {tier:<10} {bar:<20} WR={wr:.1%}  avg={avg:+.2f}%  n={len(t)}")
    print()
    print("  If 90-100 WR < 60-74 WR → scoring is inverted → retraining on")
    print("  actual returns should improve calibration over time.")


if __name__ == "__main__":
    import sys
    if "--diagnose" in sys.argv or "-d" in sys.argv:
        diagnose()
    elif "--force" in sys.argv or "-f" in sys.argv:
        retrain()
    else:
        retrain_if_due()
