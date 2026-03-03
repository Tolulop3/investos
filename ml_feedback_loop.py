"""
InvestOS — ML Feedback Loop
============================
Converts real outcome data into ML training data.
This is what makes the model self-improving over time.

HOW IT WORKS:
  Every day: outcome_tracker.py logs picks + resolves yesterday's picks
  Monthly:   this module reads ALL resolved outcomes → rebuilds training_data.json
             Next run of ml_engine.py loads that file instead of synthetic bootstrap

RESULT:
  Month 1:  ~60 real outcomes  → model still mostly synthetic (blended)
  Month 3:  ~180 real outcomes → model 50% real data
  Month 6:  ~360 real outcomes → model mostly real, bootstrap phased out
  Month 12: ~720 real outcomes → fully real, self-improving system

CALLED FROM: run_daily.py (monthly, on 1st of each month)
"""

import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, date

OUTCOMES_FILE    = "outcomes_log.json"
TRAINING_FILE    = "training_data.json"
FEEDBACK_LOG     = "feedback_log.json"

# Minimum real outcomes before we blend real + synthetic
# Below this threshold: pure synthetic (bootstrap)
# Above this threshold: blend real outcomes in
MIN_REAL_OUTCOMES = 30

# How much to weight real data vs synthetic bootstrap
# At 30 outcomes: 30% real / 70% synthetic
# At 100 outcomes: 70% real / 30% synthetic  
# At 200+ outcomes: 100% real, synthetic dropped
def get_real_weight(n_real):
    if n_real < MIN_REAL_OUTCOMES:
        return 0.0
    elif n_real >= 200:
        return 1.0
    else:
        return min(1.0, (n_real - MIN_REAL_OUTCOMES) / 170)


# ============================================================
# FEATURE FETCHER
# Re-fetches current features for a resolved ticker
# Uses same logic as ml_engine.build_features_for_stock()
# ============================================================

def fetch_features_for_ticker(ticker, rs_rating=50):
    """
    Fetch current features for a ticker.
    For historical outcomes, we use features at resolution time
    (not signal time) — best we can do without a data warehouse.
    """
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
               f"{urllib.parse.quote(ticker)}?interval=1mo&range=18mo")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode())

        closes = [c for c in data['chart']['result'][0]['indicators']['quote'][0]['close'] if c]
        if len(closes) < 8:
            return None

        # Momentum
        mom_6m  = (closes[-2] - closes[-8])  / closes[-8]  if len(closes) >= 8  else 0
        mom_12m = (closes[-2] - closes[-14]) / closes[-14] if len(closes) >= 14 else 0

        # Volatility
        daily_rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, min(91, len(closes)))]
        vol_90d = (sum(r**2 for r in daily_rets) / len(daily_rets)) ** 0.5 * (252 ** 0.5) if daily_rets else 0.2

        # Beta approximation
        beta = min(vol_90d / 0.15, 3.0)

        # RS rating normalized
        rs_norm = rs_rating / 100

        return {
            "momentum_6m":    round(mom_6m, 4),
            "momentum_12m":   round(mom_12m, 4),
            "volatility_90d": round(vol_90d, 4),
            "beta":           round(beta, 4),
            "rs_rating":      round(rs_norm, 4),
            # Fundamentals — use defaults if not available
            # (outcome_tracker doesn't store them yet)
            "roe":            0.15,
            "profit_margin":  0.10,
            "earnings_yield": 0.05,
            "fcf_yield":      0.04,
            "rev_growth":     0.08,
            "earn_growth":    0.10,
            "div_yield":      0.02,
            "debt_equity":    0.40,
            "market_regime":  1,   # Will be overridden
            "sector_momentum": 0.0,
        }

    except Exception as e:
        return None


# ============================================================
# BOOTSTRAP GENERATOR
# Same synthetic data as ml_engine.py — keeps continuity
# ============================================================

def generate_bootstrap_samples(n=2000):
    """
    Regenerate the same synthetic bootstrap data.
    Used to blend with real outcomes during early months.
    """
    try:
        import numpy as np
        import pandas as pd
    except ImportError:
        return [], []

    np.random.seed(42)

    X_data = {
        "momentum_6m":    np.random.normal(0.05, 0.15, n).tolist(),
        "momentum_12m":   np.random.normal(0.08, 0.20, n).tolist(),
        "roe":            np.random.beta(2, 5, n).tolist(),
        "profit_margin":  np.random.beta(1.5, 4, n).tolist(),
        "earnings_yield": (np.random.beta(2, 3, n) * 0.15).tolist(),
        "fcf_yield":      (np.random.beta(1.5, 4, n) * 0.10).tolist(),
        "volatility_90d": (np.random.beta(2, 5, n) * 0.6 + 0.1).tolist(),
        "beta":           np.random.normal(1.0, 0.4, n).clip(0.2, 3.0).tolist(),
        "rev_growth":     np.random.normal(0.08, 0.20, n).tolist(),
        "earn_growth":    np.random.normal(0.10, 0.30, n).tolist(),
        "div_yield":      (np.random.beta(1.5, 6, n) * 0.10).tolist(),
        "debt_equity":    np.random.beta(2, 3, n).tolist(),
        "rs_rating":      np.random.uniform(0, 1, n).tolist(),
        "market_regime":  np.random.choice([0, 1], n, p=[0.3, 0.7]).tolist(),
        "sector_momentum": np.random.normal(0, 0.10, n).tolist(),
    }

    import pandas as pd
    X = pd.DataFrame(X_data)
    score = (
        X["momentum_6m"]    * 0.20 +
        X["momentum_12m"]   * 0.15 +
        X["roe"]            * 0.15 +
        X["profit_margin"]  * 0.10 +
        X["earnings_yield"] * 0.10 +
        X["rs_rating"]      * 0.15 +
        X["market_regime"]  * 0.10 +
        X["rev_growth"]     * 0.08 -
        X["volatility_90d"] * 0.08 -
        X["debt_equity"]    * 0.05 +
        np.random.normal(0, 0.05, n)
    )
    y = (score > score.median()).astype(int)

    X_list = X.to_dict(orient="records")
    y_list = y.tolist()
    return X_list, y_list


# ============================================================
# CORE FEEDBACK FUNCTION
# This is the missing piece — outcomes → training data
# ============================================================

def build_training_data_from_outcomes(verbose=True):
    """
    Main function. Called monthly from run_daily.py.
    
    Reads outcomes_log.json → fetches features → writes training_data.json
    
    Returns dict with stats about the rebuild.
    """
    if verbose:
        print("\n" + "="*55)
        print("  ML FEEDBACK LOOP — REBUILDING TRAINING DATA")
        print("="*55)

    # 1. Load resolved outcomes
    if not os.path.exists(OUTCOMES_FILE):
        print("   ⚠️  No outcomes file found — skipping feedback loop")
        return {"status": "skipped", "reason": "no outcomes file"}

    with open(OUTCOMES_FILE) as f:
        all_outcomes = json.load(f)

    resolved = [o for o in all_outcomes
                if o.get("resolved") and o.get("outcome") in ("WIN", "LOSS", "FLAT")]

    if verbose:
        print(f"   📊 Total resolved outcomes: {len(resolved)}")

    if len(resolved) < MIN_REAL_OUTCOMES:
        print(f"   ⏳ Only {len(resolved)} outcomes — need {MIN_REAL_OUTCOMES} minimum")
        print(f"      System still using bootstrap data. Check back next month.")
        return {
            "status":       "insufficient_data",
            "real_count":   len(resolved),
            "needed":       MIN_REAL_OUTCOMES,
        }

    # 2. Build real training samples from outcomes
    real_X = []
    real_y = []
    skipped = 0
    fetched = 0

    if verbose:
        print(f"\n   🔄 Fetching features for {len(resolved)} resolved picks...")

    for outcome in resolved:
        ticker = outcome.get("ticker")
        if not ticker:
            skipped += 1
            continue

        # Label: WIN = 1, LOSS or FLAT = 0
        # We treat FLAT as 0 (didn't outperform)
        label = 1 if outcome["outcome"] == "WIN" else 0

        # Try to get stored features first (future improvement)
        # For now, fetch current features as proxy
        features = fetch_features_for_ticker(ticker, rs_rating=50)

        if features is None:
            skipped += 1
            continue

        # Override market_regime using signal_date context
        # If signal was in a bull year, set regime=1, else 0
        # Simple heuristic — improves over time
        signal_date = outcome.get("signal_date", "")
        features["market_regime"] = 1  # Default bull — refine later

        # Store the actual return as metadata (not used in training directly
        # but useful for future weighted training)
        features["_actual_return"] = outcome.get("actual_return", 0)
        features["_ticker"] = ticker
        features["_signal_date"] = signal_date

        # Clean features — remove metadata keys before storing
        clean_features = {k: v for k, v in features.items()
                         if not k.startswith("_")}

        real_X.append(clean_features)
        real_y.append(label)
        fetched += 1

        time.sleep(0.05)  # Rate limit — be gentle with Yahoo Finance

    if verbose:
        wins_in_data = sum(real_y)
        print(f"   ✅ Built {fetched} real training samples")
        print(f"   📈 Win rate in training data: {wins_in_data/max(1,fetched)*100:.1f}%")
        if skipped:
            print(f"   ⚠️  Skipped {skipped} (no data available)")

    # 3. Blend real + synthetic based on how much real data we have
    real_weight = get_real_weight(fetched)
    synthetic_weight = 1.0 - real_weight

    if verbose:
        print(f"\n   ⚖️  Data blend: {real_weight*100:.0f}% real / {synthetic_weight*100:.0f}% synthetic")

    final_X = list(real_X)
    final_y = list(real_y)

    if synthetic_weight > 0:
        # How many synthetic samples to include
        n_synthetic = int(fetched * (synthetic_weight / max(real_weight, 0.01)))
        n_synthetic = min(n_synthetic, 2000)  # Cap at 2000

        if verbose:
            print(f"   🔧 Adding {n_synthetic} synthetic samples for stability")

        synth_X, synth_y = generate_bootstrap_samples(n=n_synthetic)
        final_X.extend(synth_X)
        final_y.extend(synth_y)

    # 4. Write training_data.json in format ml_engine expects
    training_data = {
        "X": final_X,
        "y": final_y,
        "metadata": {
            "built_at":         datetime.now().isoformat(),
            "real_samples":     fetched,
            "synthetic_samples": len(final_X) - fetched,
            "total_samples":    len(final_X),
            "real_weight_pct":  round(real_weight * 100, 1),
            "win_rate_pct":     round(sum(real_y) / max(1, len(real_y)) * 100, 1),
            "outcomes_used":    len(resolved),
            "outcomes_skipped": skipped,
        }
    }

    with open(TRAINING_FILE, "w") as f:
        json.dump(training_data, f)

    if verbose:
        print(f"\n   💾 Saved {len(final_X)} training samples → {TRAINING_FILE}")
        print(f"   🧠 ML model will load this on next run")

    # 5. Log this rebuild event
    log_feedback_event(training_data["metadata"])

    return training_data["metadata"]


# ============================================================
# FEEDBACK LOG
# Tracks each monthly rebuild so you can see the system learning
# ============================================================

def log_feedback_event(metadata):
    """Track each rebuild in feedback_log.json for dashboard display."""
    log = []
    if os.path.exists(FEEDBACK_LOG):
        try:
            with open(FEEDBACK_LOG) as f:
                log = json.load(f)
        except Exception:
            log = []

    log.append(metadata)

    # Keep last 24 months
    log = log[-24:]

    with open(FEEDBACK_LOG, "w") as f:
        json.dump(log, f, indent=2)


# ============================================================
# SHOULD WE RETRAIN TODAY?
# Called from run_daily.py — only rebuilds on 1st of month
# ============================================================

def should_retrain_today():
    """
    Returns True if today is the 1st of the month.
    Also returns True if training_data.json doesn't exist yet
    and we have enough outcomes to build it.
    """
    today = date.today()

    # Always retrain on 1st of month
    if today.day == 1:
        return True

    # Also retrain if file doesn't exist but we have enough data
    if not os.path.exists(TRAINING_FILE):
        if os.path.exists(OUTCOMES_FILE):
            try:
                with open(OUTCOMES_FILE) as f:
                    outcomes = json.load(f)
                resolved = [o for o in outcomes if o.get("resolved")]
                if len(resolved) >= MIN_REAL_OUTCOMES:
                    return True
            except Exception:
                pass

    return False


def get_feedback_status():
    """
    Returns current status of the feedback loop.
    Used by dashboard to show model maturity.
    """
    status = {
        "training_file_exists": os.path.exists(TRAINING_FILE),
        "outcomes_count": 0,
        "real_weight_pct": 0,
        "last_rebuild": None,
        "next_rebuild": None,
        "model_maturity": "Bootstrap",
    }

    if os.path.exists(OUTCOMES_FILE):
        try:
            with open(OUTCOMES_FILE) as f:
                outcomes = json.load(f)
            resolved = [o for o in outcomes if o.get("resolved")]
            status["outcomes_count"] = len(resolved)
        except Exception:
            pass

    if os.path.exists(TRAINING_FILE):
        try:
            with open(TRAINING_FILE) as f:
                td = json.load(f)
            meta = td.get("metadata", {})
            status["real_weight_pct"]  = meta.get("real_weight_pct", 0)
            status["last_rebuild"]     = meta.get("built_at", "")
            status["real_samples"]     = meta.get("real_samples", 0)
            status["total_samples"]    = meta.get("total_samples", 0)

            rw = meta.get("real_weight_pct", 0)
            if rw == 0:
                status["model_maturity"] = "🔧 Bootstrap (synthetic data)"
            elif rw < 50:
                status["model_maturity"] = f"📈 Learning ({rw:.0f}% real data)"
            elif rw < 100:
                status["model_maturity"] = f"🧠 Maturing ({rw:.0f}% real data)"
            else:
                status["model_maturity"] = "✅ Fully trained on real outcomes"
        except Exception:
            pass

    if os.path.exists(FEEDBACK_LOG):
        try:
            with open(FEEDBACK_LOG) as f:
                log = json.load(f)
            if log:
                status["last_rebuild"] = log[-1].get("built_at", "")
        except Exception:
            pass

    # Next rebuild = 1st of next month
    today = date.today()
    if today.month == 12:
        next_rebuild = date(today.year + 1, 1, 1)
    else:
        next_rebuild = date(today.year, today.month + 1, 1)
    status["next_rebuild"] = next_rebuild.isoformat()

    return status


# ============================================================
# STANDALONE TEST
# ============================================================

if __name__ == "__main__":
    print("ML Feedback Loop — standalone test")
    print(f"\nStatus: {json.dumps(get_feedback_status(), indent=2)}")
    print(f"\nShould retrain today? {should_retrain_today()}")

    if should_retrain_today():
        result = build_training_data_from_outcomes(verbose=True)
        print(f"\nResult: {json.dumps(result, indent=2)}")
    else:
        print("\nNot retraining today (not 1st of month)")
        print("To force a rebuild: call build_training_data_from_outcomes() directly")
