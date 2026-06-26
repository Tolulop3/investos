"""
strategy_version.py
-------------------
Logs the exact rule state of InvestOS on every daily run.
Written to strategy_version.json in the repo root.

PURPOSE: Out-of-sample validation anchor.
  - Jun 25 2026 = v4.1 = Day 0 of frozen rule set
  - Any change to a constant below = new version = new section
  - Never tune these in response to live performance until Sep 2026 at earliest
  - future changes increment VERSION and log a new entry

HOW TO USE:
  Call log_strategy_version() near the end of run_daily.py (after factor report)
  Compare strategy_version.json entries over time to know exactly what rules
  were active for any given pick — makes September's analysis clean.
"""

import json, os
from datetime import datetime, timezone


# ── FROZEN RULE SET — v4.1 (Jun 25 2026) ────────────────────────────────────
# DO NOT CHANGE THESE VALUES without incrementing VERSION and adding a change note.
# These are the exact thresholds active at the out-of-sample start date.

VERSION = "4.1"
OOS_START_DATE = "2026-06-25"   # Out-of-sample Day 0 — freeze point
# v4.1 includes the curve steepening (0.6→0.4) — shipped Day 0-1 before any
# OOS picks resolved. Zero data discarded. Baseline is clean from this point.

RULES = {

    # ── Scoring / caps ──────────────────────────────────────────────────────
    "score_cap_74": [
        "F", "DXCM", "WPM.TO", "FM.TO",
        "ABX.TO", "AEM.TO", "AGI.TO", "MDB", "MSFT",
    ],
    "score_cap_74_note": "Chronic 90-100 losers capped at 74. Still eligible for 60-74 tier.",
    "diminishing_returns_above": 85,
    "diminishing_returns_factor": 0.4,   # tightened Jun 25 — was 0.6, too soft
    "diminishing_returns_note": (
        "Raw 100 → 91 (was 94). Raw 92 → 87.8 (tier shift to 75-89). "
        "Preserves ordinal ordering and ML gate diagnostic unlike hard ceiling."
    ),
    "news_adjustment_cap_pts": 8,

    # ── ML gate ─────────────────────────────────────────────────────────────
    "ml_gate_score_threshold": 90,
    "ml_gate_ml_prob_threshold": 0.20,
    "ml_gate_note": "score≥90 AND ml_prob<0.20 → removed from sizing AND conviction. Evidence: PF 0.96→1.65.",
    "ml_gate_applies_to": ["sizing", "conviction"],

    # ── Position sizing ─────────────────────────────────────────────────────
    "sizing_blend": "33% Kelly + 33% vol-targeted + 33% ML-proportional",
    "kelly_fraction": 0.50,            # half-Kelly
    "target_vol": 0.20,                # 20% annualised
    "max_single_formula": "max(0.20, 1.5/n_picks)",   # dynamic — scales with basket
    "max_hard_formula":   "max(0.20, 1.5/n_picks)",

    # ── Sector diversity cap ────────────────────────────────────────────────
    "sector_cap_max_per_sector": 2,
    "sector_block_net_score_threshold": -200,   # sectors below this → zero allocation

    # ── Loss-streak cooldown ─────────────────────────────────────────────────
    "loss_streak_min_losses": 2,
    "loss_streak_min_loss_pct": 1.5,
    "loss_streak_lookback_picks": 10,
    "loss_streak_cooldown_days": 7,

    # ── Permanent exclusions ─────────────────────────────────────────────────
    "permanent_exclusions_file": "cooldown_flags.json",
    "permanent_exclusions_note": "7 tickers blocked to 2036 based on factor_investigation.py section 5",

    # ── Unified regime engine ────────────────────────────────────────────────
    "regime_weights": {
        "market": 0.40,    # SPX vs 200MA
        "macro":  0.30,    # news signals
        "health": 0.30,    # rolling Sharpe
    },
    "regime_thresholds": {
        "RISK_ON":               0.50,
        "NEUTRAL":               0.10,
        "DEFENSIVE":            -0.20,
        "CAPITAL_PRESERVATION": -9.99,   # below DEFENSIVE
    },
    "regime_exposure": {
        "RISK_ON":               1.00,
        "NEUTRAL":               0.75,
        "DEFENSIVE":             0.50,
        "CAPITAL_PRESERVATION":  0.25,
    },

    # ── Sharpe Guard ─────────────────────────────────────────────────────────
    "sharpe_guard_min_threshold": 0.3,
    "sharpe_guard_trigger_below": 0.0,   # fires when Sharpe < 0
    "sharpe_guard_sizing_factor": 0.12,  # 12% of normal when fired
    "sharpe_guard_note": "Guard auto-disengages when Sharpe > 0.3",

    # ── Risk multiplier (PCR conflict) ───────────────────────────────────────
    "risk_multiplier_rules": {
        "price_bull_pcr_bearish_both":    0.25,
        "convergence_and_conflict":        0.50,
        "convergence_only":                0.50,
        "conflict_only":                   0.75,
        "clean":                           1.00,
    },
    "pcr_bearish_signals": ["BEARISH", "EXTREME_FEAR"],

    # ── PF drift monitor ─────────────────────────────────────────────────────
    "pf_baseline_file": "pf_baseline.json",
    "pf_alert_threshold": 0.20,   # drift >0.20 from baseline = investigate
    "pf_baseline_date": "2026-06-25",
    "pf_baseline_values": {
        "90-100":   0.91,
        "75-89":    1.07,
        "60-74":    1.92,
        "below-60": 1.09,
    },

    # ── Universe ─────────────────────────────────────────────────────────────
    "universe_static_count": 175,
    "universe_dynamic_file": "universe_current.json",
    "universe_dynamic_source": "scout_agent.py (Sunday 6AM)",
    "universe_removed": ["EEM", "ZCN.TO"],   # yfinance 404

    # ── OOS validation commitment ─────────────────────────────────────────────
    "oos_commitment": (
        "Rules frozen at v4.1 from Jun 25 2026. "
        "No rule changes in response to live performance until Sep 25 2026 minimum. "
        "First OOS reading: Sep 25 2026 (~90 days, ~60-70 new resolved picks). "
        "Bug fixes and non-strategy changes (UI, security, logging) are exempt."
    ),
}


def log_strategy_version(outcomes_path="outcomes_log.json"):
    """
    Appends today's rule snapshot to strategy_version.json.
    Safe to call every run — deduplicates by date.
    """
    sv_path = "strategy_version.json"

    # Load existing
    history = []
    if os.path.exists(sv_path):
        try:
            with open(sv_path) as f:
                history = json.load(f)
        except Exception:
            history = []

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Count resolved picks since OOS start
    oos_resolved = 0
    try:
        with open(outcomes_path) as f:
            picks = json.load(f)
        oos_resolved = sum(
            1 for p in picks
            if p.get("outcome") is not None
            and (p.get("signal_date") or p.get("date") or "") >= OOS_START_DATE
        )
    except Exception:
        pass

    entry = {
        "date":         today,
        "version":      VERSION,
        "oos_start":    OOS_START_DATE,
        "oos_days":     (datetime.now(timezone.utc).date() -
                         datetime.fromisoformat(OOS_START_DATE).date()).days,
        "oos_resolved_picks": oos_resolved,
        "rules":        RULES,
        "logged_at":    datetime.now(timezone.utc).isoformat(),
    }

    # Deduplicate by date — replace if same date exists
    history = [e for e in history if e.get("date") != today]
    history.append(entry)

    # Keep last 365 entries
    history = history[-365:]

    with open(sv_path, "w") as f:
        json.dump(history, f, indent=2)

    days_oos = entry["oos_days"]
    print(f"\n  📋 Strategy version: v{VERSION} | OOS Day {days_oos} | {oos_resolved} resolved picks since {OOS_START_DATE}")
    return entry


if __name__ == "__main__":
    # Standalone test
    result = log_strategy_version()
    print(f"\nRule set logged:")
    print(f"  Version:        {result['version']}")
    print(f"  OOS start:      {result['oos_start']}")
    print(f"  OOS day:        {result['oos_days']}")
    print(f"  Resolved picks: {result['oos_resolved_picks']}")
    print(f"  Rules captured: {len(result['rules'])} parameters")
