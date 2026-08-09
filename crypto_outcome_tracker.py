"""
crypto_outcome_tracker.py — InvestOS Crypto Signal Outcome Tracker
======================================================================
crypto_engine.py has scored BTC/SOL daily since inception with no way to
know whether any of it was ever right -- same gap as FX (see
fx_outcome_tracker.py's module docstring for the full audit context,
2026-08-09). This is the crypto counterpart, same log/resolve/classify/
report shape, adapted for crypto_engine.py's actual return structure:

  - crypto_engine.py returns "assets" (a dict keyed by symbol), not a
    pre-filtered "active_calls" list like fx_engine.py does -- an "active
    call" here is derived using the module's OWN actionability bar
    (conviction >= 65, the threshold crypto_engine.py's v2 recalibration
    already established for BUY/ADD and SELL/REDUCE actions -- see its
    "52% is noise not signal" comment). Below that, action is WATCH/WAIT,
    not a real prediction to grade.
  - hold_period comes from tech["hold_days"], a number (typically 7-30),
    not FX's free-text range -- still resolved on a flat window below
    rather than per-signal, same reasoning as fx_outcome_tracker.py.

Files:
  crypto_outcomes.json — all logged/resolved crypto signal entries

CALIBRATION NOTE: RESOLUTION_DAYS and WIN_LOSS_THRESHOLD_PCT are a
first-pass, not calibrated against real resolved crypto outcomes (there
are none yet). Crypto is dramatically more volatile than FX -- the
threshold below is deliberately much wider than fx_outcome_tracker.py's
0.3%. Revisit once real resolutions accumulate.
"""

import json
import os
from datetime import datetime

CRYPTO_OUTCOMES_FILE = "crypto_outcomes.json"

# crypto_engine.py's own hold_days typically runs 7-30; a single flat
# resolution window still keeps this simple (same principle as NGX/ETF/FX
# trackers) -- 14 days sits inside that range without favoring either end.
RESOLUTION_DAYS = 14

# Crypto's typical multi-day swings are an order of magnitude larger than
# FX's -- 3% is a first-pass guess at "a real directional move, not
# noise" for BTC/SOL over ~2 weeks. PROVISIONAL, see module docstring.
WIN_LOSS_THRESHOLD_PCT = 3.0

# Matches crypto_engine.py's own actionability bar (see module docstring) --
# below this, direction is a lean, not a call worth grading.
ACTIVE_CALL_MIN_CONVICTION = 65


def _classify_crypto_outcome(actual_return_pct, direction):
    """The one place crypto WIN/LOSS/FLAT classification happens -- never
    duplicate this. Direction-aware, same as fx_outcome_tracker.py's
    _classify_fx_outcome."""
    signed_pct = actual_return_pct if direction == "LONG" else -actual_return_pct
    if signed_pct > WIN_LOSS_THRESHOLD_PCT:
        return "WIN"
    elif signed_pct < -WIN_LOSS_THRESHOLD_PCT:
        return "LOSS"
    else:
        return "FLAT"


def load_crypto_outcomes():
    if os.path.exists(CRYPTO_OUTCOMES_FILE):
        try:
            return json.load(open(CRYPTO_OUTCOMES_FILE))
        except Exception:
            pass
    return []


def save_crypto_outcomes(outcomes):
    try:
        json.dump(outcomes, open(CRYPTO_OUTCOMES_FILE, "w"), indent=2, default=str)
    except Exception as e:
        print(f"   ⚠️  Crypto outcome save failed: {e}")


def log_crypto_signals(crypto_result, run_time=None):
    """
    Log today's active (conviction >= ACTIVE_CALL_MIN_CONVICTION,
    direction != NEUTRAL) crypto calls for future resolution. Called
    after run_crypto_engine() returns.

    entry_price is the price crypto_engine.py already fetched this run --
    no new fetch needed, same principle as every other tracker in this
    codebase.
    """
    if not crypto_result:
        return 0

    assets = crypto_result.get("assets", {})
    if not assets:
        return 0

    outcomes  = load_crypto_outcomes()
    today_str = datetime.now().strftime("%Y-%m-%d")
    now       = run_time or datetime.now().isoformat()

    logged_today = {o["symbol"] for o in outcomes if o.get("signal_date") == today_str}

    new_logged = 0
    for symbol, a in assets.items():
        if symbol in logged_today:
            continue
        direction  = a.get("direction", "NEUTRAL")
        conviction = a.get("conviction", 0) or 0
        if direction == "NEUTRAL" or conviction < ACTIVE_CALL_MIN_CONVICTION:
            continue  # not a real call -- WATCH/WAIT, nothing to grade

        entry_price = a.get("entry") or a.get("price")
        if entry_price is None:
            continue  # never captured a valid entry price -- nothing to resolve later

        entry = {
            "symbol":           symbol,
            "name":             a.get("name", symbol),
            "signal_date":      today_str,
            "signal_time":      now,
            "direction":        direction,
            "conviction_at_signal": conviction,
            "entry_price":      entry_price,
            "target":           a.get("target"),
            "stop":             a.get("stop"),
            "hold_days_stated": a.get("hold_period"),
            "key_driver":       a.get("key_driver"),

            # Resolution fields (filled in after RESOLUTION_DAYS)
            "resolved":          False,
            "resolved_date":     None,
            "exit_price":        None,
            "actual_return_pct": None,
            "outcome":           None,   # WIN | LOSS | FLAT
        }
        outcomes.append(entry)
        new_logged += 1
        logged_today.add(symbol)

    save_crypto_outcomes(outcomes)
    if new_logged:
        print(f"   📝 Crypto outcomes: logged {new_logged} calls ({len(outcomes)} total)")
    return new_logged


def resolve_crypto_outcomes(current_prices=None):
    """
    Check unresolved crypto calls. Resolve after RESOLUTION_DAYS (14
    calendar days). current_prices is an optional {symbol: price} dict
    (e.g. from a fresh crypto_engine scoring pass this run) -- for any
    symbol not in it, falls back to a live fetch via
    crypto_engine.fetch_crypto_data(symbol) if that function exists, same
    fetch crypto_engine.py already uses for scoring.

    Never silently defaults a missing price to a fake outcome -- leaves
    unresolved and retries next run, same principle as every other
    outcome tracker in this codebase.
    """
    outcomes = load_crypto_outcomes()
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

        symbol      = o["symbol"]
        entry_price = o.get("entry_price")
        if entry_price is None or entry_price <= 0:
            continue  # never captured a valid entry price -- can't resolve

        exit_price = current_prices.get(symbol)
        if exit_price is None:
            try:
                import crypto_engine as _ce
                fetcher = getattr(_ce, "fetch_crypto_data", None)
                if fetcher:
                    price_data = fetcher(symbol)
                    exit_price = price_data.get("price") if price_data else None
            except Exception:
                exit_price = None

        if exit_price is None:
            continue  # fetch failed -- leave unresolved, retry next run

        actual_return_pct = (exit_price - entry_price) / entry_price * 100

        o["resolved"]          = True
        o["resolved_date"]     = today.isoformat()
        o["exit_price"]        = round(exit_price, 4)
        o["actual_return_pct"] = round(actual_return_pct, 3)
        o["outcome"]           = _classify_crypto_outcome(actual_return_pct, o.get("direction"))

        resolved += 1

    save_crypto_outcomes(outcomes)
    if resolved:
        print(f"   ✅ Crypto outcomes: resolved {resolved} calls")
    return resolved


def crypto_outcome_summary():
    """Win-rate report, overall and per-symbol."""
    outcomes = load_crypto_outcomes()
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

    by_symbol = {}
    for o in resolved:
        s = o.get("symbol", "unknown")
        by_symbol.setdefault(s, []).append(o)
    by_symbol = {s: _stats(rows) for s, rows in by_symbol.items()}

    return {
        "total_logged": len(outcomes),
        "pending":      len(pending),
        "overall":      overall,
        "by_symbol":    by_symbol,
    }


def print_crypto_outcome_report():
    """Print formatted crypto outcome report."""
    s = crypto_outcome_summary()
    if not s:
        print("   No crypto outcomes logged yet")
        return

    print(f"\n{'='*50}")
    print(f"  CRYPTO SIGNAL OUTCOMES")
    print(f"{'='*50}")
    print(f"  Total logged: {s['total_logged']}  |  Pending: {s['pending']}")

    ov = s["overall"]
    if ov["total_resolved"] > 0:
        print(f"\n  OVERALL WIN RATE: {ov['win_rate']}% "
              f"({ov['wins']}W / {ov['losses']}L / {ov['flats']}F, n={ov['total_resolved']})")
    else:
        print(f"\n  OVERALL: no resolved calls yet "
              f"(calls resolve after {RESOLUTION_DAYS} days)")

    if s["by_symbol"]:
        print(f"\n  BY SYMBOL:")
        for symbol, stats in sorted(s["by_symbol"].items(), key=lambda x: -x[1]["total_resolved"]):
            if stats["total_resolved"] > 0:
                print(f"    {symbol:<12} {stats['win_rate']:>5.1f}%  (n={stats['total_resolved']})")


if __name__ == "__main__":
    print_crypto_outcome_report()
