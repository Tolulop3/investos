"""
ngx_outcome_tracker.py — NGX Paper Signal Outcome Tracker
==========================================================
Tracks NGX paper signals from Day 1 through validation phases.

Since NGX stocks have no price data on free providers, we resolve
outcomes using macro score direction after 7 days:
  WIN  = regime still RISK_ON/NEUTRAL AND stock score ≥ 60
  LOSS = regime flipped RISK_OFF OR stock score dropped < 50
  FLAT = score between 50-60 (within noise band)

This gives the model a validation loop before Day 31 (RESTRICTED phase).
Without this, there's no way to know if the macro scoring predicts
real NGX movements before real money is committed.

Files:
  ngx_outcomes.json   — all logged/resolved signal entries
  ngx_snapshot.json   — latest macro state (written by ngx_screener)
"""

import json
import os
from datetime import datetime, date


NGX_OUTCOMES_FILE = "ngx_outcomes.json"


# ── I/O helpers ──────────────────────────────────────────────────────────────

def load_ngx_outcomes():
    if os.path.exists(NGX_OUTCOMES_FILE):
        try:
            return json.load(open(NGX_OUTCOMES_FILE))
        except Exception:
            pass
    return []


def save_ngx_outcomes(outcomes):
    try:
        json.dump(outcomes, open(NGX_OUTCOMES_FILE, "w"), indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  NGX outcome save failed: {e}")


# ── Log today's NGX signals ───────────────────────────────────────────────────

def log_ngx_signals(ngx_result, run_time=None):
    """
    Log today's NGX signals for future resolution.
    Called after run_ngx_screen() returns.

    Only logs signals (not watch list) during PAPER phase.
    Captures macro context at signal time for later comparison.
    """
    if not ngx_result:
        return 0

    signals = ngx_result.get("signals", [])
    if not signals:
        return 0

    outcomes  = load_ngx_outcomes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now       = run_time or datetime.now().isoformat()

    # Don't double-log same signals on same date
    logged_today = {
        o["ticker"] for o in outcomes
        if o.get("signal_date") == today_str and o.get("resolved") is False
    }

    # Capture macro context at signal time
    macro_at_signal = {
        "regime":       ngx_result.get("macro_regime", "NEUTRAL"),
        "macro_score":  ngx_result.get("macro_score", 0),
        "fx_stress":    ngx_result.get("fx_stress", 0),
        "brent_trend":  ngx_result.get("brent_trend", "FLAT"),
        "basket":       ngx_result.get("basket_regime", "UNKNOWN"),
    }

    new_logged = 0
    for sig in signals:
        ticker = sig.get("ticker")
        if not ticker or ticker in logged_today:
            continue

        entry = {
            "ticker":           ticker,
            "name":             sig.get("name", ticker.replace(".LG", "")),
            "sector":           sig.get("sector", "unknown"),
            "tier":             sig.get("tier", 2),
            "signal_date":      today_str,
            "signal_time":      now,
            "score_at_signal":  sig.get("score", 0),
            "persistence":      sig.get("persistence", ""),
            "phase":            ngx_result.get("phase", "PAPER_ONLY"),
            "phase_days":       ngx_result.get("phase_days", 0),
            "macro_at_signal":  macro_at_signal,

            # Resolution fields (filled in after 7 days)
            "resolved":         False,
            "resolved_date":    None,
            "score_at_resolve": None,
            "regime_at_resolve":None,
            "outcome":          None,   # WIN | LOSS | FLAT
            "outcome_reason":   None,
        }
        outcomes.append(entry)
        new_logged += 1

    save_ngx_outcomes(outcomes)
    if new_logged:
        print(f"   📝 NGX outcomes: logged {new_logged} signals ({len(outcomes)} total)")
    return new_logged


# ── Resolve matured signals ───────────────────────────────────────────────────

def resolve_ngx_outcomes(ngx_result):
    """
    Resolve signals that are ≥14 days old.
    Changed from 7 to 14 days: oil commodity cycles are longer than a week.
    The 0% win rate on 7-day resolution was because Brent reversed during
    the window — 14 days gives the macro thesis time to prove itself.

    Uses current macro state (from today's ngx_result) as exit condition.
    """
    if not ngx_result:
        return 0

    outcomes  = load_ngx_outcomes()
    today     = date.today()
    resolved  = 0

    # Build current score map from all_scored
    current_scores = {
        s["ticker"]: s["score"]
        for s in ngx_result.get("all_scored", [])
    }
    current_regime = ngx_result.get("macro_regime", "NEUTRAL")

    # Debug: show what we're working with (first unresolved outcome)
    unresolved = [o for o in outcomes if not o.get("resolved")]
    if unresolved:
        sample = unresolved[0]
        try:
            sd = datetime.strptime(str(sample.get("signal_date", "")), "%Y-%m-%d").date()
            dp = (today - sd).days
            print(f"   [NGX resolve] today={today} | first_signal={sample.get('signal_date')} "
                  f"| days={dp} | ticker={sample.get('ticker')} "
                  f"| resolved_field={repr(sample.get('resolved'))}")
        except Exception as _de:
            print(f"   [NGX resolve] debug error: {_de} | sample={sample}")

    for o in outcomes:
        # Explicit bool check — handles both Python False and JSON false
        if o.get("resolved") is True:
            continue

        try:
            signal_date = datetime.strptime(
                str(o.get("signal_date", "")), "%Y-%m-%d"
            ).date()
        except ValueError:
            # Bad date format — skip but log
            print(f"   [NGX resolve] bad signal_date: {o.get('signal_date')}")
            continue

        days_passed = (today - signal_date).days

        if days_passed < 14:
            continue  # not yet — need 14 days (oil cycles > 1 week)

        ticker        = o.get("ticker", "")
        score_entry   = o.get("score_at_signal", 0)
        score_exit    = current_scores.get(ticker)
        regime_exit   = current_regime

        o["resolved"]          = True
        o["resolved_date"]     = today.isoformat()
        o["score_at_resolve"]  = score_exit
        o["regime_at_resolve"] = regime_exit

        # Determine outcome
        if score_exit is None:
            o["outcome"]        = "LOSS"
            o["outcome_reason"] = "Ticker no longer scoring — macro shifted"
        elif regime_exit == "RISK_OFF":
            o["outcome"]        = "LOSS"
            o["outcome_reason"] = f"Macro flipped RISK_OFF (score now {score_exit})"
        elif score_exit >= 65:
            o["outcome"]        = "WIN"
            o["outcome_reason"] = f"Score held strong: {score_entry}→{score_exit} | {regime_exit}"
        elif score_exit >= 50:
            o["outcome"]        = "FLAT"
            o["outcome_reason"] = f"Score softened: {score_entry}→{score_exit} | {regime_exit}"
        else:
            o["outcome"]        = "LOSS"
            o["outcome_reason"] = f"Score collapsed: {score_entry}→{score_exit} | {regime_exit}"

        resolved += 1

    save_ngx_outcomes(outcomes)
    if resolved:
        print(f"   ✅ NGX outcomes: resolved {resolved} signals")
    return resolved


# ── Summary report ────────────────────────────────────────────────────────────

def ngx_outcome_summary():
    """
    Print NGX paper signal performance summary.
    Returns dict with key metrics.
    """
    outcomes  = load_ngx_outcomes()
    if not outcomes:
        return {}

    resolved  = [o for o in outcomes if o.get("resolved")]
    pending   = [o for o in outcomes if not o.get("resolved")]

    wins    = sum(1 for o in resolved if o.get("outcome") == "WIN")
    losses  = sum(1 for o in resolved if o.get("outcome") == "LOSS")
    flats   = sum(1 for o in resolved if o.get("outcome") == "FLAT")
    total_r = len(resolved)

    win_rate = round(wins / total_r * 100, 1) if total_r > 0 else 0

    # By sector
    sector_wins = {}
    sector_total = {}
    for o in resolved:
        sec = o.get("sector", "unknown")
        sector_total[sec] = sector_total.get(sec, 0) + 1
        if o.get("outcome") == "WIN":
            sector_wins[sec] = sector_wins.get(sec, 0) + 1

    # By tier
    t1_resolved = [o for o in resolved if o.get("tier") == 1]
    t1_wins     = sum(1 for o in t1_resolved if o.get("outcome") == "WIN")
    t1_wr       = round(t1_wins / len(t1_resolved) * 100, 1) if t1_resolved else 0

    summary = {
        "total_logged":  len(outcomes),
        "total_resolved":total_r,
        "pending":       len(pending),
        "wins":          wins,
        "losses":        losses,
        "flats":         flats,
        "win_rate":      win_rate,
        "t1_win_rate":   t1_wr,
        "sector_wins":   sector_wins,
        "sector_total":  sector_total,
    }

    return summary


def print_ngx_outcome_report():
    """Print formatted NGX outcome report."""
    s = ngx_outcome_summary()
    if not s:
        print("   No NGX outcomes logged yet")
        return

    print(f"\n{'='*50}")
    print(f"  NGX PAPER SIGNAL OUTCOMES")
    print(f"{'='*50}")
    print(f"  Total logged:    {s['total_logged']}")
    print(f"  Resolved:        {s['total_resolved']}")
    print(f"  Pending (7d):    {s['pending']}")

    if s['total_resolved'] > 0:
        print(f"\n  WIN RATE:        {s['win_rate']}%  "
              f"({s['wins']}W / {s['losses']}L / {s['flats']}F)")
        print(f"  Tier 1 WR:       {s['t1_win_rate']}%")

        if s['sector_total']:
            print(f"\n  BY SECTOR:")
            for sec, total in sorted(s['sector_total'].items(),
                                     key=lambda x: -x[1]):
                wins  = s['sector_wins'].get(sec, 0)
                wr    = round(wins / total * 100) if total else 0
                bar   = "█" * (wr // 10)
                print(f"    {sec:<12} {bar:<10} {wr}%  ({wins}/{total})")
    else:
        print(f"\n  No resolved signals yet — check back after Day 7")
    print(f"{'='*50}")
    return s
