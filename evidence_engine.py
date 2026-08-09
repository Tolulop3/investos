"""
Evidence Engine — InvestOS v4.0

Converts "Score: 94" into "412 similar setups, 61.2% WR, +8.6% avg return."

This is Stage 2 of the evidence-backed rules architecture:
  Stage 1: Rules exist (done)
  Stage 2: Rules show their evidence (this file)
  Stage 3: Replace rules with learned models when N > 10,000 (future)

Every output from this engine answers:
  "Historically, when a stock looked like this in a similar regime,
   what actually happened?"
"""

import json
import os
from datetime import datetime

OUTCOMES_FILE = "outcomes_log.json"
MIN_OBSERVATIONS = 20   # minimum before we show evidence (don't bluff with 5 obs)
SCORE_WINDOW    = 8     # ±8 points around pick's score counts as "similar"


def _load_outcomes():
    try:
        return json.load(open(OUTCOMES_FILE))
    except Exception:
        return []


def _regime_matches(outcome_regime, current_regime):
    """Loose regime match — RISK_ON matches RISK_ON, NEUTRAL matches NEUTRAL."""
    if not outcome_regime or not current_regime:
        return True   # can't filter → include
    return outcome_regime.strip().upper() == current_regime.strip().upper()


def lookup_evidence(pick_score, current_regime, outcomes=None, spx_90d_return=None):
    """
    For a pick with a given score, find all historical outcomes in a similar
    score band and return statistical evidence.

    Args:
        pick_score:       composite score (0-100) of this pick
        current_regime:   unified_regime string e.g. "RISK_ON"
        outcomes:         pre-loaded outcomes list (avoids re-reading file)
        spx_90d_return:   SPX 90-day return % for alpha computation

    Returns dict:
        count           int    — number of similar historical setups
        win_rate        float  — % of similar setups that were wins
        avg_return      float  — average actual_return across similar setups
        median_return   float  — median actual_return
        max_drawdown    float  — worst actual_return in similar setups
        regime_match_pct float — % of similar setups in same regime
        expected_alpha  float  — avg_return minus spx_90d_return (if available)
        confidence      str    — HIGH / MODERATE / LOW / INSUFFICIENT
        score_band      str    — e.g. "86-94" (what we searched)
        evidence_text   str    — human-readable summary
    """
    if outcomes is None:
        outcomes = _load_outcomes()

    resolved = [o for o in outcomes
                if o.get("actual_return") is not None
                and o.get("outcome") in ("WIN", "LOSS", "FLAT")]

    if not resolved:
        return _no_evidence(pick_score)

    # Score band: ±SCORE_WINDOW around pick_score, clamped 0-100
    lo = max(0,   pick_score - SCORE_WINDOW)
    hi = min(100, pick_score + SCORE_WINDOW)

    similar = [o for o in resolved if lo <= o.get("score", 0) <= hi]

    if len(similar) < MIN_OBSERVATIONS:
        return _no_evidence(pick_score, count=len(similar), band=(lo, hi))

    # Basic stats
    wins   = [o for o in similar if o.get("outcome") == "WIN"]
    returns = [o["actual_return"] for o in similar]
    win_rate = len(wins) / len(similar) * 100

    sorted_r = sorted(returns)
    n = len(sorted_r)
    median = (sorted_r[n//2] + sorted_r[(n-1)//2]) / 2

    avg_return   = sum(returns) / len(returns)
    max_drawdown = min(returns)

    # Regime breakdown
    regime_matches = [o for o in similar if _regime_matches(o.get("unified_regime"), current_regime)]
    regime_match_pct = len(regime_matches) / len(similar) * 100 if similar else 0

    # Regime-filtered stats (more relevant)
    if len(regime_matches) >= MIN_OBSERVATIONS:
        regime_returns  = [o["actual_return"] for o in regime_matches]
        regime_wr       = len([o for o in regime_matches if o.get("outcome")=="WIN"]) / len(regime_matches) * 100
        regime_avg      = sum(regime_returns) / len(regime_returns)
    else:
        regime_wr  = win_rate
        regime_avg = avg_return

    # Expected alpha vs benchmark
    expected_alpha = None
    if spx_90d_return is not None:
        expected_alpha = round(regime_avg - spx_90d_return, 2)

    # Confidence tier
    n_obs = len(similar)
    if n_obs >= 200:
        confidence = "HIGH"
    elif n_obs >= 80:
        confidence = "MODERATE"
    elif n_obs >= MIN_OBSERVATIONS:
        confidence = "LOW"
    else:
        confidence = "INSUFFICIENT"

    # Evidence text (what shows on dashboard)
    alpha_str = f" | Expected alpha: {expected_alpha:+.1f}% vs SPX" if expected_alpha is not None else ""
    regime_note = f" ({len(regime_matches)} in same regime)" if len(regime_matches) >= MIN_OBSERVATIONS else ""
    evidence_text = (
        f"{n_obs} similar setups{regime_note} | "
        f"WR: {regime_wr:.1f}% | "
        f"Avg: {regime_avg:+.1f}%"
        f"{alpha_str}"
    )

    # Rule validation — is this setup's evidence supporting or contradicting a BUY?
    # This is where evidence makes existing rules transparent:
    rule_note = None
    if pick_score >= 90 and regime_wr < 52:
        rule_note = (
            f"⚠️  High Score Confirmation Rule: {n_obs} historical setups "
            f"at this score level produced only {regime_wr:.1f}% WR. "
            f"ML confirmation required before BUY."
        )
    elif pick_score >= 75 and regime_wr >= 58:
        rule_note = f"✅ Evidence supports: {regime_wr:.1f}% WR over {n_obs} setups"

    return {
        "count":             n_obs,
        "regime_count":      len(regime_matches),
        "win_rate":          round(win_rate, 1),
        "regime_win_rate":   round(regime_wr, 1),
        "avg_return":        round(avg_return, 2),
        "regime_avg_return": round(regime_avg, 2),
        "median_return":     round(median, 2),
        "max_drawdown":      round(max_drawdown, 2),
        "regime_match_pct":  round(regime_match_pct, 1),
        "expected_alpha":    expected_alpha,
        "confidence":        confidence,
        "score_band":        f"{lo:.0f}-{hi:.0f}",
        "evidence_text":     evidence_text,
        "rule_note":         rule_note,
    }


def _no_evidence(score, count=0, band=None):
    lo, hi = band if band else (max(0,score-SCORE_WINDOW), min(100,score+SCORE_WINDOW))
    return {
        "count":             count,
        "regime_count":      0,
        "win_rate":          None,
        "regime_win_rate":   None,
        "avg_return":        None,
        "regime_avg_return": None,
        "median_return":     None,
        "max_drawdown":      None,
        "regime_match_pct":  None,
        "expected_alpha":    None,
        "confidence":        "INSUFFICIENT",
        "score_band":        f"{lo:.0f}-{hi:.0f}",
        "evidence_text":     f"Insufficient history (n={count}) for score band {lo:.0f}-{hi:.0f}",
        "rule_note":         None,
    }


def enrich_picks_with_evidence(picks, current_regime, spx_90d_return=None, verbose=True):
    """
    Add evidence block to each pick. Called after ML engine, before conviction engine.
    Mutates each pick dict in-place, adds 'evidence' key.
    Returns enriched picks.
    """
    outcomes = _load_outcomes()
    enriched = 0

    for pick in picks:
        score = pick.get("score", 70)
        ev = lookup_evidence(score, current_regime, outcomes, spx_90d_return)
        pick["evidence"] = ev
        if ev["confidence"] not in ("INSUFFICIENT",):
            enriched += 1

    if verbose and picks:
        print(f"  📊 Evidence Engine: {enriched}/{len(picks)} picks have historical backing")
        # Print evidence for top 3
        top = sorted(picks, key=lambda x: x.get("score",0), reverse=True)[:3]
        for p in top:
            ev = p.get("evidence", {})
            if ev.get("confidence") not in ("INSUFFICIENT",):
                print(f"     {p.get('ticker','?'):<10} {ev.get('evidence_text','')}")
            if ev.get("rule_note"):
                print(f"     {'':10} {ev['rule_note']}")

    return picks


def get_tier_evidence_summary(outcomes=None):
    """
    Return the tier-level evidence table — the foundation for all versioned rules.
    This is what gets shown on the dashboard's evidence panel.
    """
    if outcomes is None:
        outcomes = _load_outcomes()

    resolved = [o for o in outcomes if o.get("actual_return") is not None
                and o.get("outcome") in ("WIN","LOSS","FLAT")]

    tiers = [
        ("90-100", 90, 100),
        ("75-89",  75,  89),
        ("60-74",  60,  74),
        ("below-60", 0, 59),
    ]

    summary = {}
    for label, lo, hi in tiers:
        tp = [o for o in resolved if lo <= o.get("score",0) <= hi]
        if not tp:
            continue
        wins   = [o for o in tp if o.get("outcome")=="WIN"]
        rets   = [o["actual_return"] for o in tp]
        wr     = len(wins)/len(tp)*100
        avg    = sum(rets)/len(rets)
        sorted_r = sorted(rets)
        n = len(sorted_r)
        med = (sorted_r[n//2] + sorted_r[(n-1)//2]) / 2

        # Versioned hypothesis metadata
        rule_status = "ACTIVE"
        rule_threshold = None
        rule_description = None
        if label == "90-100" and wr < 52:
            rule_description = f"Require ML ≥ 20% before BUY (WR only {wr:.1f}%)"
            rule_threshold = 20.0
        elif label == "60-74" and wr >= 60:
            rule_description = f"Sweet spot: {wr:.1f}% WR. Full sizing authorized."

        summary[label] = {
            "count":          len(tp),
            "win_rate":       round(wr, 1),
            "avg_return":     round(avg, 2),
            "median_return":  round(med, 2),
            "worst_return":   round(min(rets), 2),
            "best_return":    round(max(rets), 2),
            "rule_status":    rule_status,
            "rule_description": rule_description,
            "rule_threshold": rule_threshold,
            "evidence_since": "June 2026",
        }

    return summary
