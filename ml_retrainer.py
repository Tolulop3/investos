"""
InvestOS — ML Retrainer v2
===========================
Reads outcomes_log.json (1,480+ resolved picks) and retrains XGBoost
using actual 5-day forward returns as the label.

v2 changes:
  - FEATURES matches ml_engine.py ML_CONFIG exactly (18 features)
    Original had 18 features too but was written before ml_engine.py was confirmed
    This version is the source of truth — feature list kept in sync with ML_CONFIG
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
RETRAIN_LOCK   = "ml_last_retrain.txt"

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
    "sector_momentum",
    "market_regime",
    "spx_vs_ma200",
    "news_boost",
    "close_to_ema20_ratio",  # overextension signal added 2026-07-04
    # NOTE: unified_regime_num and market_breadth are at 0% coverage in
    # outcomes_log.json — not added until logged. Sector (100% coverage) skipped
    # because it requires one-hot encoding — add in a dedicated session.
    # Also: joblib is not installed so ml_model_cache.pkl cannot be loaded by
    # ml_engine.py — prediction always uses the heuristic formula in predict().
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
    # Monotonic constraints: +1=higher is better, -1=higher is worse
    # Only applied to features with clear directional relationships to alpha
    "monotone_constraints": {
        "roe":                  1,   # higher ROE → better stock
        "profit_margin":        1,   # higher margin → better stock
        "earnings_yield":       1,   # higher E/P → cheaper → better
        "volatility_90d":      -1,   # higher vol → worse risk-adj returns
        "close_to_ema20_ratio": -1,  # more overbought → lower forward return
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
    print(f"  📂 {OUTCOMES_FILE}: {len(raw)} total rows, {len(resolved)} resolved")
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

    X = pd.DataFrame(rows_X)[FEATURES]
    y = pd.Series(rows_y, dtype=int)
    w = np.array(weights, dtype=float)

    pos_rate = float(y.mean())
    print(f"  ✅ Feature matrix: {len(y)} rows × {len(FEATURES)} features")
    print(f"     Label balance: {pos_rate:.1%} positive | {1-pos_rate:.1%} negative")

    # ── COVERAGE GATE ─────────────────────────────────────────────────────
    # Historical picks (pre feature-capture era) have perf_90d=0, roe=0 etc.
    # Training on all-zero rows → zero-variance model → AUC=0.500 (random).
    # Guard: require ≥10% of rows have real feature data before retraining.
    # New picks (from June 20, 2026+) capture full feature snapshots.
    key_features = ["momentum_6m", "roe", "profit_margin"]  # rs_rating excluded — default 50 inflates coverage
    has_real_data = X[key_features].abs().sum(axis=1) > 0.001
    coverage_pct  = float(has_real_data.mean()) * 100
    real_rows     = int(has_real_data.sum())
    print(f"  📊 Feature coverage: {coverage_pct:.1f}% ({real_rows}/{len(y)} rows have real data)")

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
    print(f"  ✅ Coverage {coverage_pct:.1f}% ≥ {MIN_COVERAGE_PCT}% — proceeding with retrain")
    # ──────────────────────────────────────────────────────────────────────

    dates_arr = np.array(dates_list)
    return X, y, w, dates_arr


def train_and_save(X, y, w, dates=None):
    if not (HAS_XGB and HAS_SKLEARN and HAS_JOBLIB and HAS_PANDAS):
        print("  ⚠️  Missing libraries — cannot train.")
        return None

    n     = len(y)
    split = int(n * 0.8)
    X_train, X_val = X.iloc[:split], X.iloc[split:]
    y_train, y_val = y.iloc[:split], y.iloc[split:]
    w_train        = w[:split]

    # ── 90-day purge buffer ───────────────────────────────────────────────
    # Prevents training on data within 90 days of the validation start date.
    # Guards against lookahead via long-lookback features (90d vol, 200d MA).
    PURGE_DAYS = 90
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

    scaler    = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)
    X_all_s   = scaler.transform(X)

    n_splits  = min(5, max(3, n // 50))
    tscv      = TimeSeriesSplit(n_splits=n_splits)
    cv_aucs   = []

    for fold_train_idx, fold_val_idx in tscv.split(X_all_s):
        try:
            fm = XGBClassifier(**XGB_PARAMS)
            fm.fit(X_all_s[fold_train_idx], y.iloc[fold_train_idx],
                   sample_weight=w[fold_train_idx], verbose=False)
            fa = roc_auc_score(y.iloc[fold_val_idx],
                               fm.predict_proba(X_all_s[fold_val_idx])[:,1])
            cv_aucs.append(fa)
        except Exception:
            pass

    cv_mean = float(np.mean(cv_aucs)) if cv_aucs else 0.5
    cv_std  = float(np.std(cv_aucs))  if cv_aucs else 0.0
    print(f"  📊 CV AUC: {cv_mean:.3f} ± {cv_std:.3f} ({n_splits} folds)")

    model = XGBClassifier(**XGB_PARAMS)
    model.fit(X_train_s, y_train, sample_weight=w_train,
              eval_set=[(X_val_s, y_val)], verbose=False)

    val_probs   = model.predict_proba(X_val_s)[:,1]
    holdout_auc = roc_auc_score(y_val, val_probs) if len(y_val.unique())>1 else 0.5
    print(f"  📊 Holdout AUC: {holdout_auc:.3f}  "
          f"({'✅ better than random' if holdout_auc > 0.53 else '⚠️ near random — more data needed'})")

    try:
        calibrator = CalibratedClassifierCV(model, method="isotonic", cv="prefit")
        calibrator.fit(X_train_s, y_train)
        print("  ✅ Probability calibration: isotonic")
    except Exception:
        calibrator = None

    feat_imp = {}
    if hasattr(model, "feature_importances_"):
        feat_imp = dict(sorted(
            zip(FEATURES, [round(float(v),4) for v in model.feature_importances_]),
            key=lambda x: x[1], reverse=True
        ))

    # Calibration check
    calib_check = {}
    try:
        all_probs = calibrator.predict_proba(X_all_s)[:,1] if calibrator else                     model.predict_proba(X_all_s)[:,1]
        df_check  = pd.DataFrame({"prob": all_probs, "label": y.values})
        df_check["decile"] = pd.qcut(df_check["prob"], q=5,
                                      labels=["Q1","Q2","Q3","Q4","Q5"],
                                      duplicates="drop")
        for q, grp in df_check.groupby("decile", observed=True):
            calib_check[str(q)] = {
                "mean_pred_prob":  round(float(grp["prob"].mean()), 3),
                "actual_win_rate": round(float(grp["label"].mean()), 3),
                "count": len(grp),
            }
    except Exception:
        pass

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
        "scaler":             scaler,
        "calibrator":         calibrator,
        "feature_importance": feat_imp,
        "feature_hash":       feat_hash,
        "_trained_on":        "real_outcomes",  # tells ml_engine.py this is not synthetic
        "_retrained_at":      datetime.now().isoformat(),
        "_n_training_rows":   len(y_train),
        "_holdout_auc":       round(holdout_auc, 4),
        "_cv_auc":            round(cv_mean, 4),
        "_label_definition":  "actual_return > median(actual_return)",
        "_features":          FEATURES,
    }
    joblib.dump(payload, MODEL_CACHE)
    print(f"  💾 Saved to {MODEL_CACHE} (feature_hash: {feat_hash})")

    report = {
        "retrained_at":       datetime.now().isoformat(),
        "n_rows":             n,
        "n_train":            split,
        "n_val":              n - split,
        "holdout_auc":        round(holdout_auc, 4),
        "cv_auc_mean":        round(cv_mean, 4),
        "cv_auc_std":         round(cv_std, 4),
        "feature_importance": feat_imp,
        "calibration_check":  calib_check,
        "features_used":      FEATURES,
        "label_definition":   "actual_return > median(actual_return)",
    }
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
