"""
strategy_version.py
-------------------
Logs the exact rule state of InvestOS on every daily run.
Written to strategy_version.json in the repo root.

PURPOSE: Out-of-sample validation anchor.
  - Jun 25 2026 = v4.1 = Day 0 of frozen rule set
  - Aug 15 2026 = v4.2 = Day 0 of a NEW frozen rule set (freeze reset — see
    "v4.2 OVERRIDE" note below; the Sep 25 2026 v4.1 freeze target was not honored)
  - Any change to a constant below = new version = new section
  - Never tune these in response to live performance until ~90 days after
    the current version's Day 0 (v4.2: Day 0 = Aug 15 2026, target ~Nov 2026)
  - future changes increment VERSION and log a new entry

# ── v4.2 OVERRIDE NOTE (2026-08-15) ──────────────────────────────────────────
# The v4.1 freeze committed to no rule changes before Sep 25 2026. This
# version breaks that commitment early. Documenting why rather than doing
# it silently:
#   Root-caused a "score-tier inversion" (90-100 tier picks underperforming
#   60-74 tier). Traced to the momentum pillar cap having been raised
#   20->35 on 2026-05-03 ("V2") on the strength of check_momentum_factor_health()
#   reporting "healthy" — but that check only measures whether SCORE
#   correlates with PRICE, which is near-tautological (score already
#   includes momentum) and never validated momentum against future
#   win/loss. Measured directly against 1,103 resolved 90-100-tier picks:
#   within that tier, WINNERS averaged LOWER perf_90d/perf_30d/rs_rating
#   than LOSERS — momentum was inversely predictive exactly where V2
#   weighted it hardest. Combined with an uncapped bonus layer, this put
#   56% of the entire 90-100 tier at score=100 exactly, where score had
#   no ranking power left (winners 97.31 vs losers 97.89 avg).
#   This is a correction of a flawed original diagnosis, not a tune
#   chasing live-performance noise — but it IS a rule change inside the
#   freeze window, so the freeze clock resets from today rather than
#   pretending Sep 25 2026 still applies to the old (now-superseded) rules.

# ── DOWNSTREAM GAP INVESTIGATION (2026-08-15) — NEGATIVE RESULT, DO NOT
#    RE-INVESTIGATE THE SAME WAY ────────────────────────────────────────────
#   Before shipping v4.2, checked whether the residual 90-100 tier
#   underperformance (~42% WR vs ~57% for 75-89/60-74, on real historical
#   score_stock() replays that stayed monotonic) was explained by downstream
#   filters: ML gate (score>=90 AND ml_prob<0.20), sector-first gate
#   (ml_engine.py SECTOR_ALLOW/SECTOR_BLOCK), and the >=75 sector block.
#   Naive test (applying today's gate rules retroactively to ALL historical
#   dates) suggested they explained a real chunk (42.2%->45.3% WR). WRONG —
#   these mechanisms didn't exist for the whole window: no gate at all
#   before 2026-06-21, ML-prob-only gate 2026-06-21 to 07-03, sector gate +
#   materials-only >=75 block 07-04 to 07-07, full 4-sector >=75 block from
#   07-08 on (gate_engine.py itself didn't exist until 07-09). Re-run
#   properly scoped to each rule's actual active window: 90-100 tier WR
#   corrects to ~41.5% — statistically indistinguishable from the raw,
#   ungated 42.2%. Isolating to era 4 (07-08 on, the only period where
#   "today's rules" are valid to apply) showed the gate's own EXCLUSIONS
#   (n=32, 59.4% WR, mostly HEALTHCARE) outperformed what it KEPT (n=88,
#   40.9% WR, 66% FINANCIALS) — the opposite of what the gate's own PF
#   evidence would predict. Most likely small-sample noise (single-digit
#   per-sector sub-buckets), not a real miscalibration — but either way,
#   the downstream-gate hypothesis does not explain the residual gap.
#   CONCLUSION: the ~12-15pt residual 90-100-tier gap (after v4.2's pillar
#   fix, and net of everything the ML/sector gates already catch) has NO
#   identified cause as of this writing. Candidates not yet ruled out:
#   sector cap (max 2/sector, diversification-driven not quality-driven,
#   scoped to TFSA only and hard to isolate from outcomes_log's account-
#   ambiguous category field), reserve/substitution pool specifics, and
#   gate_engine.py hysteresis (day-over-day memory beyond a flat ml_prob
#   threshold). Closing further requires full per-date code+state
#   reconstruction across stock_screener.py/ml_engine.py/gate_engine.py,
#   not just point-in-time snapshots — a materially bigger project than
#   this session's dig. Revisit at the Nov 2026 OOS read with real v4.2
#   forward data before re-attempting this backward-looking archaeology.

# ── SHARPE SLIDE EXPLAINED (2026-08-15) — MECHANICAL, NOT ONGOING DEGRADATION ──
#   Rolling Sharpe fell from +0.12 (Jul 23) to -0.32 (Aug 15), 23 straight days
#   of decline. Before reading this as the system currently getting worse:
#   it's a rolling-90-day-window artifact of ONE already-known event, not new
#   or ongoing. Trailing-window avg return by "as of" date: Jul23 +0.11% ->
#   Jul31 +0.04% -> Aug3 -0.02% -> Aug8 -0.17% -> Aug15 -0.27%. Mechanism:
#   strongly positive early-May weeks (WR 63-69%) are rolling OFF the back of
#   the window as it advances, while the week of Jun 22-28 (n=341 resolved,
#   ~3x normal weekly volume, avg return -3.72%, WR 29.6%) stays anchored
#   inside the window the whole time. That week traces to signals from Jun
#   12-21, broad across every sector (not a category or stock-picking
#   failure) and lines up exactly with a real SPX drawdown (7d return
#   +3.21% on Jun18-21 -> -2.60% Jun24 -> -2.05% Jun25, staying negative
#   through Jun29) — the SAME event already in this file's PERFORMANCE
#   HISTORY / the README's ("Jun 26 — Guard re-engaged, DEFENSIVE 0.25x").
#   Confirmed independently: win_rate.json's trailing windows show the last
#   14 days alone (clean of the Jun event) at +0.86% avg return, 48.1% WR —
#   fine. It's only the 60d window still carrying the Jun 22-28 tail that's
#   negative. CONCLUSION: this reading will mechanically self-correct as
#   the Jun 22-28 cohort rolls out of the 90-day window around Sep 20-26,
#   independent of anything v4.2 does. Do not treat the Aug 15 Sharpe
#   reading as evidence v4.2 (or anything else) is failing — it predates
#   v4.2 entirely. Do re-check this after Sep 26: if Sharpe does NOT
#   recover once this cohort rolls off, that would be a genuine new signal
#   worth investigating (not yet done — this note only explains the past
#   23 days, not what happens next).

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

VERSION = "4.2"
OOS_START_DATE = "2026-08-15"   # Out-of-sample Day 0 — v4.2 pillar rebalance live
# HISTORY: Jun 25 was tagged v4.1 but the curve fix (0.6→0.4) had NOT shipped.
# BAC scored 94.2 on Jun 26 — confirmed old curve still active.
# Reset to Jun 26: the actual first run with 0.4 multiplier live.
# Jun 25 picks (2 logged) excluded from OOS count — correct baseline.
# v4.1 OOS window (Jun 26 - Aug 15 2026) closed early — superseded by v4.2
# (see "v4.2 OVERRIDE NOTE" above). Prior VERSION = "4.1".
PRIOR_VERSION = "4.1"

RULES = {

    # ── Scoring / caps ──────────────────────────────────────────────────────
    "score_cap_74": [
        "F", "DXCM", "WPM.TO", "FM.TO",
        "ABX.TO", "AEM.TO", "AGI.TO", "MDB", "MSFT",
    ],
    "score_cap_74_note": "Chronic 90-100 losers capped at 74. Still eligible for 60-74 tier.",
    "diminishing_returns_above": 85,
    "diminishing_returns_factor": 0.4,   # tightened Jun 26 — was 0.6 (Jun 25 tag was wrong, fix not in code)
    "diminishing_returns_note": (
        "Raw 100 → 91 (was 94). Raw 92 → 87.8 (tier shift to 75-89). "
        "Preserves ordinal ordering and ML gate diagnostic unlike hard ceiling."
    ),
    "news_adjustment_cap_pts": 8,

    # ── Pillar caps (v4.2, 2026-08-15) ───────────────────────────────────────
    # Rebalanced from V2 (2026-05-03: momentum 20->35, growth 20->15,
    # value 15->12, safety 15->13) back toward pre-V2 values, plus a new
    # bonus cap. See "v4.2 OVERRIDE NOTE" above for the empirical basis.
    "pillar_cap_momentum_pts":         22,   # was 35 (V2), pre-V2 was 20
    "pillar_cap_growth_pts":           20,   # was 15 (V2), restored to pre-V2
    "pillar_cap_value_pts":            16,   # was 12 (V2), above pre-V2's 15
    "pillar_cap_safety_pts":           15,   # was 13 (V2), restored to pre-V2
    "pillar_cap_dividend_income_pts":  15,   # unchanged — V2's cut here wasn't contradicted by the data
    "pillar_cap_volume_liquidity_pts": 10,   # unchanged
    "bonus_cap_pts":                   15,   # new — bonus was previously unbounded upward
    "geometric_mean_momentum_signal_threshold_pts": 13,  # was 20 — rescaled proportionally (20/35 -> 13/22) so ITEM 6's "strong momentum" signal keeps firing at the same relative rate against the new, smaller momentum cap

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
        "Rules frozen at v4.2 from Aug 15 2026 (freeze reset — v4.1's Jun 25 -> "
        "Sep 25 2026 commitment was broken early; see 'v4.2 OVERRIDE NOTE'). "
        "No rule changes in response to live performance until ~Nov 15 2026 minimum. "
        "REAL OOS reading (statistically meaningful, ~90 days, ~60-70 resolved picks): "
        "~Nov 15 2026. Do not shorten this — it's sized for sample size, not a calendar "
        "preference. Interim checkpoint (directional only, informal, NOT a decision "
        "point) at ~Oct 15 2026 (~61 days, ~2/3 the sample) — added 2026-08-15 per "
        "explicit request for earlier visibility. If a future session is tempted to "
        "treat the Oct 15 checkpoint as the real read, it isn't — see this note. "
        "Bug fixes and non-strategy changes (UI, security, logging) are exempt. "
        "#4 (RISK_ON momentum weight multiplier in strategy_engine.py) explicitly "
        "deferred — do not tune it in response to v4.2 results before the REAL "
        "(Nov 15) read."
    ),

    # ── OOS Period attribution boundaries ────────────────────────────────────
    # Each period's label, start, and logic change that defines it.
    # Used to slice outcomes_log.json in attribution reports.
    # Do NOT overlap periods — end is exclusive / start is inclusive.
    "oos_periods": [
        {
            "period": 1,
            "label": "OOS — baseline",
            "start": "2026-06-26",
            "end":   "2026-06-30",   # exclusive — last day of baseline rule set
            "note":  "v4.1 frozen rule set. Diminishing-returns factor 0.4 live. Gate not yet added.",
        },
        {
            "period": 2,
            "label": "OOS — post-curve",
            "start": "2026-06-30",
            "end":   "2026-07-01",
            "note":  "Curve fix confirmed active (BAC=94.2→91). Gate still off. Very short window.",
        },
        {
            "period": 3,
            "label": "OOS — pre-gate",
            "start": "2026-07-01",
            "end":   "2026-07-01",
            "note":  "Placeholder — gate not yet live. Adjust if gate shipped before 2026-07-01.",
        },
        {
            "period": 4,
            "label": "OOS — post-gate",
            "start": "2026-07-01",
            "end":   "2026-08-15",   # closed — superseded by v4.2 pillar rebalance
            "note":  (
                "ML gate activated (score≥90 AND ml_prob<0.20 → removed from sizing+conviction). "
                "Sector diversity cap added. Reserve pool integrity fix live. "
                "First attributable gate-era picks from this date forward."
            ),
        },
        {
            "period": 5,
            "label": "OOS — v4.2 pillar rebalance",
            "start": "2026-08-15",
            "end":   None,   # open-ended — current period
            "note":  (
                "Momentum/growth/value/safety pillar caps rebalanced (22/20/16/15, "
                "was 35/15/12/13) and bonus capped at ±15 (was unbounded) — see "
                "'v4.2 OVERRIDE NOTE'. #4 (RISK_ON momentum multiplier) deferred "
                "pending this period's results."
            ),
        },
    ],

    # ── GEV resolution tracker ────────────────────────────────────────────────
    # Tracks individual high-stakes picks pending OOS resolution.
    # Format: ticker | Score | Gate path | ML prob | Outcome (PENDING/WIN/LOSS)
    "gev_resolution_tracker": [
        {
            "ticker":    "GEV",
            "score":     100,
            "sector":    "INDUSTRIALS",   # tagged INDUSTRIALS in pipeline (not Energy)
            "gate_path": "ML gate — INDUSTRIALS is neither SECTOR_ALLOW nor SECTOR_BLOCK, falls through to ML gate",
            "ml_prob":   0.50,
            "outcome":   "PENDING",
            "expected_resolution": "2026-07-11",
            "note":      "Score 100 but not ML-removed (ml_prob=0.50 ≥ 0.20 threshold). Watch for resolution Jul 11.",
        },
    ],
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
