"""
regime_predictor.py — InvestOS Regime Shift Predictor
======================================================
Uses the history/ archive to detect regime momentum.
Answers: is the current regime ACCELERATING, STABLE, or DECELERATING?

Requires history/ files (started June 6, 2026).
Needs minimum 5 days to produce a signal.

WHAT IT TRACKS:
  1. Sharpe trend      — 3-day slope of rolling Sharpe
  2. Breadth trend     — 3-day slope of pct_above_200
  3. Win rate trend    — 5-day slope of WR 30d
  4. Regime duration   — how many consecutive days in current regime

OUTPUT:
  regime_momentum: ACCELERATING | STABLE | DECELERATING
  confidence: 0.0 - 1.0
  days_in_regime: N
  signal_summary: plain text description

INTEGRATION:
  Feeds into run_daily.py as a 4th layer in the unified regime engine.
  Weight: 0.10 (small — this is a secondary confirmation, not a driver).
  Only shifts regime by one tier max (RISK_ON → NEUTRAL, not RISK_ON → CAPITAL_PRESERVATION).
"""

import json
import os
from datetime import datetime, timedelta


HISTORY_DIR  = "history"
MIN_DAYS     = 5   # minimum days of history to produce signal
SIGNAL_DAYS  = 7   # days to look back for trend


def load_history(n=SIGNAL_DAYS):
    """Load last N history snapshots. Returns list sorted oldest→newest."""
    # Try multiple path resolutions — GitHub Actions CWD can vary
    _dir = HISTORY_DIR
    if not os.path.exists(_dir):
        _alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), HISTORY_DIR)
        if os.path.exists(_alt):
            _dir = _alt
        else:
            return []

    files = sorted([
        f for f in os.listdir(_dir)
        if f.endswith(".json") and f.startswith("2026")
    ])[-n:]

    records = []
    for fname in files:
        try:
            data = json.load(open(os.path.join(_dir, fname)))
            # Normalize fields
            records.append({
                "date":        fname.replace(".json",""),
                "sharpe":      _safe_float(data, ["sharpe",
                               "risk_report.decay_monitor.rolling_sharpe.sharpe"]),
                "breadth_200": _safe_float(data, ["breadth.pct_above_200",
                               "breadth_pct_above_200"]),
                "wr_30d":      _safe_float(data, ["win_rate_30d",
                               "win_rate.windows.30d.win_rate"]),
                "regime":      _safe_str(data,   ["regime.regime",
                               "market_regime.regime"]),
                "unified":     _safe_str(data,   ["system_exposure.unified_regime",
                               "unified_regime"]),
            })
        except Exception:
            continue

    return records


def _safe_float(d, paths):
    """Try multiple dot-notation paths, return float or None."""
    for path in paths:
        try:
            val = d
            for key in path.split("."):
                val = val[key]
            if val is not None:
                return float(val)
        except Exception:
            continue
    return None


def _safe_str(d, paths):
    """Try multiple paths, return string or None."""
    for path in paths:
        try:
            val = d
            for key in path.split("."):
                val = val[key]
            if val is not None:
                return str(val)
        except Exception:
            continue
    return None


def _slope(values):
    """Simple linear slope of a list of values (last point minus first, normalized)."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return 0.0
    return (clean[-1] - clean[0]) / len(clean)


def _consecutive_regime_days(records, current_regime):
    """Count how many consecutive days the current regime has been active."""
    count = 0
    for r in reversed(records):
        if (r.get("unified") == current_regime or
                r.get("regime") == current_regime):
            count += 1
        else:
            break
    return count


def predict_regime_shift(current_unified_regime=None, verbose=True):
    """
    Analyze history/ archive and predict regime momentum.
    Returns dict with momentum signal.
    """
    records = load_history(SIGNAL_DAYS)

    if len(records) < MIN_DAYS:
        result = {
            "momentum":       "STABLE",
            "confidence":     0.0,
            "days_in_regime": 0,
            "days_of_history": len(records),
            "note":           f"Need {MIN_DAYS} days minimum ({len(records)} available)",
            "sharpe_slope":   0.0,
            "breadth_slope":  0.0,
            "wr_slope":       0.0,
        }
        if verbose:
            print(f"  ⏳ Regime predictor: {len(records)}/{MIN_DAYS} days — building history")
        return result

    # ── Extract time series ───────────────────────────────────────────────────
    sharpes   = [r["sharpe"]      for r in records]
    breadths  = [r["breadth_200"] for r in records]
    wr_30ds   = [r["wr_30d"]      for r in records]

    sharpe_slope  = _slope(sharpes)    # per day change in Sharpe
    breadth_slope = _slope(breadths)   # per day change in breadth %
    wr_slope      = _slope(wr_30ds)    # per day change in WR 30d

    # ── Regime duration ───────────────────────────────────────────────────────
    days_in_regime = _consecutive_regime_days(records, current_unified_regime or "")

    # ── Signal scoring ────────────────────────────────────────────────────────
    # Each slope contributes to an acceleration score
    # Positive = improving = ACCELERATING regime
    # Negative = deteriorating = DECELERATING regime

    acc_score = 0.0

    # Sharpe trend (most important — direct system health)
    if sharpe_slope > 0.005:     acc_score += 1.5   # improving 0.005/day
    elif sharpe_slope > 0.002:   acc_score += 0.8
    elif sharpe_slope < -0.005:  acc_score -= 1.5
    elif sharpe_slope < -0.002:  acc_score -= 0.8

    # Breadth trend (market participation)
    if breadth_slope > 0.5:      acc_score += 1.0   # >0.5% per day breadth expansion
    elif breadth_slope > 0.2:    acc_score += 0.5
    elif breadth_slope < -0.5:   acc_score -= 1.0
    elif breadth_slope < -0.2:   acc_score -= 0.5

    # Win rate trend (system accuracy)
    if wr_slope > 0.3:           acc_score += 0.8   # WR improving 0.3%/day
    elif wr_slope > 0.1:         acc_score += 0.4
    elif wr_slope < -0.3:        acc_score -= 0.8
    elif wr_slope < -0.1:        acc_score -= 0.4

    # Regime duration modifier — long regimes near reversal
    if days_in_regime >= 20:
        # Long regime = mean reversion risk — slight downward bias
        acc_score -= 0.3
    elif days_in_regime <= 3:
        # Very new regime — uncertainty
        pass

    # ── Classify momentum ─────────────────────────────────────────────────────
    if acc_score >= 1.5:
        momentum   = "ACCELERATING"
        confidence = min(1.0, acc_score / 3.0)
        summary    = (f"Regime strengthening — Sharpe trending +{sharpe_slope:.4f}/day, "
                      f"breadth {breadth_slope:+.1f}%/day")
    elif acc_score <= -1.5:
        momentum   = "DECELERATING"
        confidence = min(1.0, abs(acc_score) / 3.0)
        summary    = (f"Regime weakening — Sharpe trending {sharpe_slope:.4f}/day, "
                      f"breadth {breadth_slope:+.1f}%/day")
    else:
        momentum   = "STABLE"
        confidence = 1.0 - abs(acc_score) / 1.5
        summary    = f"Regime stable — no significant directional trend detected"

    # ── Regime shift suggestion ───────────────────────────────────────────────
    # Only suggest shift if confidence > 0.6 AND momentum is clear
    regime_order    = ["CAPITAL_PRESERVATION","DEFENSIVE","NEUTRAL","RISK_ON"]
    shift_suggested = None

    if confidence > 0.6 and current_unified_regime in regime_order:
        idx = regime_order.index(current_unified_regime)
        if momentum == "ACCELERATING" and idx < len(regime_order) - 1:
            shift_suggested = regime_order[idx + 1]
        elif momentum == "DECELERATING" and idx > 0:
            shift_suggested = regime_order[idx - 1]

    result = {
        "momentum":         momentum,
        "confidence":       round(confidence, 2),
        "acc_score":        round(acc_score, 2),
        "days_in_regime":   days_in_regime,
        "days_of_history":  len(records),
        "sharpe_slope":     round(sharpe_slope, 5),
        "breadth_slope":    round(breadth_slope, 3),
        "wr_slope":         round(wr_slope, 3),
        "summary":          summary,
        "shift_suggested":  shift_suggested,
        "latest_sharpe":    sharpes[-1],
        "latest_breadth":   breadths[-1],
        "latest_wr_30d":    wr_30ds[-1],
    }

    if verbose:
        icon = "📈" if momentum == "ACCELERATING" else "📉" if momentum == "DECELERATING" else "➡️"
        print(f"  {icon} Regime momentum: {momentum} (confidence {confidence:.0%})")
        print(f"     {summary}")
        print(f"     Sharpe slope: {sharpe_slope:+.4f}/day | Breadth: {breadth_slope:+.2f}%/day | WR: {wr_slope:+.2f}%/day")
        print(f"     Days in {current_unified_regime or 'current'} regime: {days_in_regime}")
        if shift_suggested:
            print(f"     ⚡ Momentum suggests: {shift_suggested} (not overriding — advisory only)")

    return result


if __name__ == "__main__":
    result = predict_regime_shift(current_unified_regime="RISK_ON", verbose=True)
    print(f"\nResult: {result}")
