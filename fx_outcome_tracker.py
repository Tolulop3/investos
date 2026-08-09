"""
fx_outcome_tracker.py — InvestOS FX Signal Outcome Tracker
=============================================================
fx_engine.py has scored 5 FX/gold pairs daily since inception with no way
to know whether any of it was ever right -- confirmed via audit 2026-08-09:
every other signal type in this system (stocks, NGX, ETFs) has its own
outcome tracker; FX and crypto did not. This closes the gap for FX,
following the same log-at-signal-time -> resolve-after-N-days -> pure
classify function -> win-rate report shape as outcome_tracker.py (stocks),
ngx_outcome_tracker.py (NGX), and etf_outcome_tracker.py (ETFs) -- but as
its own file, not a reuse of any of them, because FX's schema doesn't
transfer:

  - Only LONG/SHORT calls are real predictions to grade. NEUTRAL is an
    explicit no-call (unlike ETF's full-universe logging, which exists
    specifically to avoid survivorship bias in a SCORE -- FX has no
    per-pair score to validate that way, only a directional call).
  - Only 5 pairs total, all liquid, unlike ETF's category-tiered
    volatility spread -- one flat threshold is defensible here.

Files:
  fx_outcomes.json — all logged/resolved FX signal entries

CALIBRATION NOTE: RESOLUTION_DAYS and WIN_LOSS_THRESHOLD_PCT below are a
first-pass, not calibrated against real resolved FX outcomes (there are
none yet -- that's what this file exists to start collecting). Revisit
both once enough real resolutions accumulate, same as etf_outcome_tracker.py
did for its category thresholds.
"""

import json
import os
from datetime import datetime, timedelta

FX_OUTCOMES_FILE = "fx_outcomes.json"

# A LONG/SHORT call's stated hold_period is a free-text range ("1-3 days"
# or "3-7 days", see fx_engine.py's tech signal) -- not a clean number to
# resolve against. RESOLUTION_DAYS picks a single flat window instead,
# same principle NGX (14d) and ETF (30d) used: the complexity lives in the
# threshold, not the window. 5 calendar days sits inside both stated
# ranges without favoring either.
RESOLUTION_DAYS = 5

# Flat, not category-tiered (unlike ETF) -- only 5 pairs, all liquid
# majors/gold, no defensive-vs-thematic volatility spread to account for.
# 0.3% is a first-pass guess at "a real directional move, not noise" for
# a several-day FX/gold hold -- PROVISIONAL, see module docstring.
WIN_LOSS_THRESHOLD_PCT = 0.3


def _classify_fx_outcome(actual_return_pct, direction):
    """
    The one place FX WIN/LOSS/FLAT classification happens -- never
    duplicate this. Direction-aware: for a SHORT call, a price DROP is
    the win-direction, so the sign of actual_return_pct is flipped before
    comparing to the threshold.
    """
    signed_pct = actual_return_pct if direction == "LONG" else -actual_return_pct
    if signed_pct > WIN_LOSS_THRESHOLD_PCT:
        return "WIN"
    elif signed_pct < -WIN_LOSS_THRESHOLD_PCT:
        return "LOSS"
    else:
        return "FLAT"


def load_fx_outcomes():
    if os.path.exists(FX_OUTCOMES_FILE):
        try:
            return json.load(open(FX_OUTCOMES_FILE))
        except Exception:
            pass
    return []


def save_fx_outcomes(outcomes):
    try:
        json.dump(outcomes, open(FX_OUTCOMES_FILE, "w"), indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  FX outcome save failed: {e}")


def log_fx_signals(fx_result, run_time=None):
    """
    Log today's active (LONG/SHORT) FX calls for future resolution.
    Called after run_fx_engine() returns.

    Only logs fx_result["active_calls"] -- NEUTRAL pairs are not a
    prediction (no direction to grade), so logging them would just be
    noise, unlike ETF's deliberate full-universe logging (which exists to
    validate a SCORE, not a directional call).

    entry_price is the price fx_engine.py already fetched this run (each
    pair's "price" field) -- no new fetch needed, same principle as
    ngx_outcome_tracker.py/etf_outcome_tracker.py: a price can only ever
    be captured live, right now, so a signal logged without one can never
    be resolved later.
    """
    if not fx_result:
        return 0

    active_calls = fx_result.get("active_calls", [])
    if not active_calls:
        return 0

    outcomes  = load_fx_outcomes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now       = run_time or datetime.now().isoformat()

    logged_today = {o["pair"] for o in outcomes if o.get("signal_date") == today_str}

    new_logged = 0
    for call in active_calls:
        pair = call.get("pair")
        if not pair or pair in logged_today:
            continue

        entry_price = call.get("entry") or call.get("price")
        if entry_price is None:
            continue  # never captured a valid entry price -- nothing to resolve later

        entry = {
            "pair":             pair,
            "symbol":           call.get("symbol"),
            "signal_date":      today_str,
            "signal_time":      now,
            "direction":        call.get("direction"),
            "conviction_at_signal": call.get("conviction"),
            "entry_price":      entry_price,
            "target":           call.get("target"),
            "stop":             call.get("stop"),
            "hold_period_stated": call.get("hold_period"),
            "key_driver":       call.get("key_driver"),

            # Resolution fields (filled in after RESOLUTION_DAYS)
            "resolved":          False,
            "resolved_date":     None,
            "exit_price":        None,
            "actual_return_pct": None,
            "outcome":           None,   # WIN | LOSS | FLAT
        }
        outcomes.append(entry)
        new_logged += 1
        logged_today.add(pair)

    save_fx_outcomes(outcomes)
    if new_logged:
        print(f"   📝 FX outcomes: logged {new_logged} calls ({len(outcomes)} total)")
    return new_logged


def resolve_fx_outcomes(current_prices=None):
    """
    Check unresolved FX calls. Resolve after RESOLUTION_DAYS (5 calendar
    days). current_prices is an optional {pair: price} dict (e.g. from a
    fresh fx_engine scoring pass this run) -- for any pair not in it,
    falls back to a live fetch via fx_engine.fetch_fx_data(symbol), the
    same fetch fx_engine.py already uses for scoring, so no new fetch
    mechanism is introduced here.

    Never silently defaults a missing price to a fake outcome -- leaves
    unresolved and retries next run, same principle as every other
    outcome tracker in this codebase.
    """
    outcomes = load_fx_outcomes()
    if not outcomes:
        return 0

    today = datetime.now().date()
    resolved = 0
    current_prices = current_prices or {}

    for o in outcomes:
        if o.get("resolved"):
            continue

        try:
            signal_date = datetime.strptime(o["signal_date"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
            continue

        days_passed = (today - signal_date).days
        if days_passed < RESOLUTION_DAYS:
            continue

        pair        = o["pair"]
        entry_price = o.get("entry_price")
        if entry_price is None or entry_price <= 0:
            continue  # never captured a valid entry price -- can't resolve

        exit_price = current_prices.get(pair)
        if exit_price is None:
            try:
                import fx_engine as _fe
                symbol = o.get("symbol")
                if symbol:
                    price_data = _fe.fetch_fx_data(symbol)
                    exit_price = price_data.get("price") if price_data and price_data.get("status") == "ok" else None
            except Exception:
                exit_price = None

        if exit_price is None:
            continue  # fetch failed -- leave unresolved, retry next run

        actual_return_pct = (exit_price - entry_price) / entry_price * 100

        o["resolved"]          = True
        o["resolved_date"]     = today.isoformat()
        o["exit_price"]        = round(exit_price, 5)
        o["actual_return_pct"] = round(actual_return_pct, 3)
        o["outcome"]           = _classify_fx_outcome(actual_return_pct, o.get("direction"))

        resolved += 1

    save_fx_outcomes(outcomes)
    if resolved:
        print(f"   ✅ FX outcomes: resolved {resolved} calls")
    return resolved


def fx_outcome_summary():
    """Win-rate report, overall and per-pair."""
    outcomes = load_fx_outcomes()
    if not outcomes:
        return {}

    resolved = [o for o in outcomes if o.get("resolved")]
    pending  = [o for o in outcomes if not o.get("resolved")]

    def _stats(rows):
        if not rows:
            return {"total_resolved": 0, "wins": 0, "losses": 0, "flats": 0, "win_rate": 0}
        wins   = sum(1 for o in rows if o.get("outcome") == "WIN")
        losses = sum(1 for o in rows if o.get("outcome") == "LOSS")
        flats  = sum(1 for o in rows if o.get("outcome") == "FLAT")
        return {
            "total_resolved": len(rows),
            "wins":     wins,
            "losses":   losses,
            "flats":    flats,
            "win_rate": round(wins / len(rows) * 100, 1) if rows else 0,
        }

    overall = _stats(resolved)

    by_pair = {}
    for o in resolved:
        p = o.get("pair", "unknown")
        by_pair.setdefault(p, []).append(o)
    by_pair = {p: _stats(rows) for p, rows in by_pair.items()}

    return {
        "total_logged": len(outcomes),
        "pending":      len(pending),
        "overall":      overall,
        "by_pair":      by_pair,
    }


def print_fx_outcome_report():
    """Print formatted FX outcome report."""
    s = fx_outcome_summary()
    if not s:
        print("   No FX outcomes logged yet")
        return

    print(f"\n{'='*50}")
    print(f"  FX SIGNAL OUTCOMES")
    print(f"{'='*50}")
    print(f"  Total logged: {s['total_logged']}  |  Pending: {s['pending']}")

    ov = s["overall"]
    if ov["total_resolved"] > 0:
        print(f"\n  OVERALL WIN RATE: {ov['win_rate']}% "
              f"({ov['wins']}W / {ov['losses']}L / {ov['flats']}F, n={ov['total_resolved']})")
    else:
        print(f"\n  OVERALL: no resolved calls yet "
              f"(calls resolve after {RESOLUTION_DAYS} days)")

    if s["by_pair"]:
        print(f"\n  BY PAIR:")
        for pair, stats in sorted(s["by_pair"].items(), key=lambda x: -x[1]["total_resolved"]):
            if stats["total_resolved"] > 0:
                print(f"    {pair:<15} {stats['win_rate']:>5.1f}%  (n={stats['total_resolved']})")


if __name__ == "__main__":
    print_fx_outcome_report()
